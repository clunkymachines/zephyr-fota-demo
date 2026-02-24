/*
 * SPDX-License-Identifier: Apache-2.0
 */

#include <zephyr/kernel.h>
#include <zephyr/drivers/gpio.h>
#include <zephyr/logging/log.h>
#include <zephyr/logging/log_ctrl.h>
#include <zephyr/sys/atomic.h>
#include <zephyr/sys/reboot.h>
#include <zephyr/net/socket.h>
#include <zephyr/net/http/client.h>
#include <zephyr/net/net_if.h>
#include <zephyr/net/dhcpv4.h>
#include <zephyr/net/ethernet_mgmt.h>
#include <zephyr/net/net_event.h>
#include <zephyr/net/net_mgmt.h>
#include <zephyr/dfu/flash_img.h>
#include <zephyr/dfu/mcuboot.h>
#include <errno.h>

LOG_MODULE_REGISTER(app, LOG_LEVEL_INF);

#define LED0_NODE DT_ALIAS(led0)
#define BLINK_PERIOD K_MSEC(1000) // HERE
#define UPDATE_POLL_PERIOD K_SECONDS(10)
#define UPDATE_THREAD_STACK_SIZE 4096
#define UPDATE_THREAD_PRIORITY 7

#define UPDATE_HOST "vrm.free.fr"
#define UPDATE_PORT "80"
#define UPDATE_PATH "/demo.bin"
#define UPDATE_HTTP_TIMEOUT_MS 10000
#define UPDATE_RECV_BUF_SIZE 1024

#define NET_IF_EVENT_MASK (NET_EVENT_IF_UP | NET_EVENT_IF_DOWN)
#define NET_IPV4_EVENT_MASK (NET_EVENT_IPV4_ADDR_ADD | NET_EVENT_IPV4_ADDR_DEL | \
			     NET_EVENT_IPV4_DHCP_START | NET_EVENT_IPV4_DHCP_BOUND | \
			     NET_EVENT_IPV4_DHCP_STOP)
#define NET_ETH_EVENT_MASK (NET_EVENT_ETHERNET_CARRIER_ON | NET_EVENT_ETHERNET_CARRIER_OFF)

static const struct gpio_dt_spec led = GPIO_DT_SPEC_GET(LED0_NODE, gpios);
static struct net_mgmt_event_callback net_if_cb;
static struct net_mgmt_event_callback net_ipv4_cb;
static struct net_mgmt_event_callback net_eth_cb;
static struct net_if *target_iface;
static atomic_t net_ready;
static atomic_t image_confirmed;
static atomic_t update_pending;
static atomic_t polling_started;
static atomic_t waiting_for_ipv4_logged;

struct update_download_context {
	struct flash_img_context flash_ctx;
	size_t bytes_written;
	int status_code;
	uint8_t upload_slot;
	bool status_seen;
	bool flash_initialized;
	bool download_failed;
	bool final_received;
};

static bool is_target_iface(struct net_if *iface)
{
	return iface != NULL && iface == target_iface;
}

static const char *net_event_to_str(uint64_t event)
{
	switch (event) {
	case NET_EVENT_IF_UP:
		return "IF_UP";
	case NET_EVENT_IF_DOWN:
		return "IF_DOWN";
	case NET_EVENT_IPV4_ADDR_ADD:
		return "IPV4_ADDR_ADD";
	case NET_EVENT_IPV4_ADDR_DEL:
		return "IPV4_ADDR_DEL";
	case NET_EVENT_IPV4_DHCP_START:
		return "IPV4_DHCP_START";
	case NET_EVENT_IPV4_DHCP_BOUND:
		return "IPV4_DHCP_BOUND";
	case NET_EVENT_IPV4_DHCP_STOP:
		return "IPV4_DHCP_STOP";
	case NET_EVENT_ETHERNET_CARRIER_ON:
		return "ETHERNET_CARRIER_ON";
	case NET_EVENT_ETHERNET_CARRIER_OFF:
		return "ETHERNET_CARRIER_OFF";
	default:
		return "UNKNOWN";
	}
}

static void log_ipv4_info(struct net_if *iface)
{
	bool found = false;

	for (int i = 0; i < NET_IF_MAX_IPV4_ADDR; i++) {
		char addr_buf[NET_IPV4_ADDR_LEN];
		char mask_buf[NET_IPV4_ADDR_LEN];
		char gw_buf[NET_IPV4_ADDR_LEN];
		const struct net_if_addr_ipv4 *ifaddr = &iface->config.ip.ipv4->unicast[i];

		if (!ifaddr->ipv4.is_used) {
			continue;
		}

		found = true;
		LOG_INF("IPv4 address[%d]: %s", i,
			net_addr_ntop(AF_INET, &ifaddr->ipv4.address.in_addr, addr_buf,
				      sizeof(addr_buf)));
		LOG_INF("IPv4 netmask[%d]: %s", i,
			net_addr_ntop(AF_INET, &ifaddr->netmask, mask_buf, sizeof(mask_buf)));
		LOG_INF("IPv4 gateway    : %s",
			net_addr_ntop(AF_INET, &iface->config.ip.ipv4->gw, gw_buf, sizeof(gw_buf)));
#if defined(CONFIG_NET_DHCPV4)
		if (ifaddr->ipv4.addr_type == NET_ADDR_DHCP) {
			LOG_INF("DHCP lease time : %u seconds", iface->config.dhcpv4.lease_time);
		}
#endif
	}

	if (!found) {
		LOG_INF("IPv4 address is not assigned yet on iface %d (%s)",
			net_if_get_by_iface(iface), net_if_get_device(iface)->name);
	}
}

static bool iface_has_ipv4_addr(struct net_if *iface)
{
	if (iface == NULL || iface->config.ip.ipv4 == NULL) {
		return false;
	}

	for (int i = 0; i < NET_IF_MAX_IPV4_ADDR; i++) {
		if (iface->config.ip.ipv4->unicast[i].ipv4.is_used) {
			return true;
		}
	}

	return false;
}

static void set_net_ready_state(bool ready, const char *reason)
{
	int previous = atomic_get(&net_ready);
	int next = ready ? 1 : 0;

	if (previous != next) {
		LOG_INF("Network readiness -> %s (%s)", ready ? "READY" : "NOT_READY", reason);
	}

	atomic_set(&net_ready, next);
}

static void confirm_running_image_once(void)
{
	if (!IS_ENABLED(CONFIG_BOOTLOADER_MCUBOOT)) {
		return;
	}

	if (atomic_get(&image_confirmed)) {
		return;
	}

	if (!atomic_get(&net_ready)) {
		return;
	}

	if (boot_is_img_confirmed()) {
		atomic_set(&image_confirmed, 1);
		if (atomic_get(&update_pending) && mcuboot_swap_type() == BOOT_SWAP_TYPE_NONE) {
			atomic_set(&update_pending, 0);
			LOG_INF("No MCUboot swap pending anymore; HTTP polling resumed");
		}
		LOG_INF("Current image already confirmed");
		return;
	}

	LOG_INF("Attempting to confirm running image after IPv4 readiness");

	int ret = boot_write_img_confirmed();

	if (ret < 0) {
		LOG_ERR("Failed to confirm running image (%d)", ret);
		return;
	}

	atomic_set(&image_confirmed, 1);
	/* Once this image is confirmed, MCUboot should not keep it in pending/revert path. */
	atomic_set(&update_pending, 0);
	LOG_INF("HTTP polling resumed after image confirmation");
	LOG_INF("Running image marked as tested/confirmed");
}

static int connect_http_socket(const char *host, const char *port)
{
	struct zsock_addrinfo hints = {
		.ai_family = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
		.ai_protocol = IPPROTO_TCP,
	};
	struct zsock_addrinfo *results = NULL;
	struct zsock_addrinfo *ai = NULL;
	int ret;
	int sock = -1;

	ret = zsock_getaddrinfo(host, port, &hints, &results);
	if (ret != 0) {
		LOG_WRN("DNS resolution failed for %s:%s (%d)", host, port, ret);
		return -EHOSTUNREACH;
	}

	for (ai = results; ai != NULL; ai = ai->ai_next) {
		sock = zsock_socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
		if (sock < 0) {
			continue;
		}

		ret = zsock_connect(sock, ai->ai_addr, ai->ai_addrlen);
		if (ret == 0) {
			break;
		}

		zsock_close(sock);
		sock = -1;
	}

	zsock_freeaddrinfo(results);

	if (sock < 0) {
		return -ENOTCONN;
	}

	return sock;
}

static int update_http_response_cb(struct http_response *rsp, enum http_final_call final_data,
				   void *user_data)
{
	struct update_download_context *ctx = user_data;
	bool is_final = (final_data == HTTP_DATA_FINAL);
	int status_code;
	int ret;

	if (!ctx->status_seen) {
		ctx->status_seen = true;
		ctx->status_code = rsp->http_status_code;
	}

	status_code = ctx->status_seen ? ctx->status_code : rsp->http_status_code;
	if (status_code != 200) {
		return 0;
	}

	/* http_client may signal final callback with no body fragment. */
	if (!rsp->body_found || rsp->body_frag_len == 0U) {
		if (is_final && ctx->flash_initialized) {
			ret = flash_img_buffered_write(&ctx->flash_ctx, NULL, 0, true);
			if (ret < 0) {
				ctx->download_failed = true;
				LOG_ERR("flash_img_buffered_write final flush failed (%d)", ret);
				return ret;
			}

			ctx->bytes_written = flash_img_bytes_written(&ctx->flash_ctx);
		}

		if (is_final) {
			ctx->final_received = true;
		}
		return 0;
	}

	if (!ctx->flash_initialized) {
		ctx->upload_slot = flash_img_get_upload_slot();
		ret = flash_img_init_id(&ctx->flash_ctx, ctx->upload_slot);
		if (ret < 0) {
			ctx->download_failed = true;
			LOG_ERR("flash_img_init_id(slot=%u) failed (%d)", ctx->upload_slot, ret);
			return ret;
		}

		ctx->flash_initialized = true;
		LOG_INF("Downloading update into slot %u", ctx->upload_slot);
	}

	ret = flash_img_buffered_write(&ctx->flash_ctx, (const uint8_t *)rsp->body_frag_start,
				       rsp->body_frag_len, is_final);
	if (ret < 0) {
		ctx->download_failed = true;
		LOG_ERR("flash_img_buffered_write failed (%d)", ret);
		return ret;
	}

	ctx->bytes_written = flash_img_bytes_written(&ctx->flash_ctx);
	LOG_DBG("Downloaded %u bytes so far", (unsigned int)ctx->bytes_written);
	if (is_final) {
		ctx->final_received = true;
	}

	return 0;
}

static bool is_upgrade_already_pending(void)
{
	if (!IS_ENABLED(CONFIG_BOOTLOADER_MCUBOOT)) {
		return false;
	}

	int swap_type = mcuboot_swap_type();

	if (swap_type < 0) {
		LOG_WRN("mcuboot_swap_type failed (%d)", swap_type);
		return false;
	}

	if (swap_type == BOOT_SWAP_TYPE_NONE) {
		return false;
	}

	atomic_set(&update_pending, 1);
	LOG_INF("MCUboot swap already pending (type=%d)", swap_type);
	return true;
}

static int poll_update_url_once(void)
{
	static const char *headers[] = {
		"Connection: close\r\n",
		NULL,
	};
	uint8_t recv_buf[UPDATE_RECV_BUF_SIZE];
	struct update_download_context dl_ctx = {0};
	struct http_request req = {0};
	int sock;
	int ret;

	sock = connect_http_socket(UPDATE_HOST, UPDATE_PORT);
	if (sock < 0) {
		return sock;
	}

	LOG_INF("Polling update URL: http://%s%s", UPDATE_HOST, UPDATE_PATH);

	req.method = HTTP_GET;
	req.url = UPDATE_PATH;
	req.host = UPDATE_HOST;
	req.protocol = "HTTP/1.1";
	req.response = update_http_response_cb;
	req.recv_buf = recv_buf;
	req.recv_buf_len = sizeof(recv_buf);
	req.header_fields = headers;

	ret = http_client_req(sock, &req, UPDATE_HTTP_TIMEOUT_MS, &dl_ctx);
	zsock_close(sock);
	if (ret < 0) {
		LOG_WRN("HTTP request failed (%d)", ret);
		return ret;
	}

	if (!dl_ctx.status_seen) {
		LOG_WRN("HTTP response status not parsed");
		return -EBADMSG;
	}

	if (dl_ctx.status_code != 200) {
		LOG_INF("No update image available (HTTP %d)", dl_ctx.status_code);
		return 0;
	}

	LOG_INF("Update image found (HTTP 200), downloading...");

	if (dl_ctx.download_failed || !dl_ctx.flash_initialized || !dl_ctx.final_received ||
	    dl_ctx.bytes_written == 0U) {
		LOG_ERR("Update download incomplete");
		return -EIO;
	}

	if (!IS_ENABLED(CONFIG_BOOTLOADER_MCUBOOT)) {
		LOG_ERR("MCUboot support is not enabled");
		return -ENOTSUP;
	}

	ret = boot_request_upgrade(BOOT_UPGRADE_TEST);
	if (ret < 0) {
		LOG_ERR("boot_request_upgrade failed (%d)", ret);
		return ret;
	}

	atomic_set(&update_pending, 1);
	LOG_INF("Update written (%u bytes) to slot %u; rebooting for MCUboot swap",
		(unsigned int)dl_ctx.bytes_written, dl_ctx.upload_slot);
	log_panic();
	sys_reboot(SYS_REBOOT_COLD);

	return 0;
}

static void update_thread_fn(void *arg1, void *arg2, void *arg3)
{
	ARG_UNUSED(arg1);
	ARG_UNUSED(arg2);
	ARG_UNUSED(arg3);

	while (1) {
		if (atomic_get(&net_ready)) {
			if (atomic_get(&waiting_for_ipv4_logged)) {
				atomic_set(&waiting_for_ipv4_logged, 0);
				LOG_INF("IPv4 became ready, polling/resume conditions met");
			}

			if (!atomic_get(&polling_started)) {
				atomic_set(&polling_started, 1);
				LOG_INF("HTTP polling started");
			}

			confirm_running_image_once();

			if (!atomic_get(&update_pending) && !is_upgrade_already_pending()) {
				int ret = poll_update_url_once();

				if (ret < 0) {
					LOG_WRN("Update poll failed (%d)", ret);
				}
			}
		} else if (!atomic_get(&waiting_for_ipv4_logged)) {
			atomic_set(&waiting_for_ipv4_logged, 1);
			LOG_INF("HTTP polling paused: waiting for IPv4 assignment");
		}

		k_sleep(UPDATE_POLL_PERIOD);
	}
}

K_THREAD_DEFINE(update_thread, UPDATE_THREAD_STACK_SIZE, update_thread_fn, NULL, NULL, NULL,
		UPDATE_THREAD_PRIORITY, 0, 0);

static void on_net_event(struct net_mgmt_event_callback *cb, uint64_t event, struct net_if *iface)
{
	ARG_UNUSED(cb);

	if (iface == NULL) {
		LOG_WRN("Network event without interface");
		return;
	}

	if (!is_target_iface(iface)) {
		return;
	}

	LOG_INF("Network event %s on iface %d (%s)", net_event_to_str(event),
		net_if_get_by_iface(iface), net_if_get_device(iface)->name);

	switch (event) {
	case NET_EVENT_IPV4_ADDR_ADD:
#if !defined(CONFIG_NET_DHCPV4)
		log_ipv4_info(iface);
#endif
		set_net_ready_state(true, "IPV4_ADDR_ADD");
		confirm_running_image_once();
		break;
	case NET_EVENT_IPV4_DHCP_BOUND:
		log_ipv4_info(iface);
		set_net_ready_state(true, "IPV4_DHCP_BOUND");
		confirm_running_image_once();
		break;
	case NET_EVENT_IPV4_ADDR_DEL:
		set_net_ready_state(iface_has_ipv4_addr(iface), "IPV4_ADDR_DEL");
		break;
	case NET_EVENT_IPV4_DHCP_STOP:
	case NET_EVENT_IF_DOWN:
		set_net_ready_state(false, net_event_to_str(event));
		break;
	case NET_EVENT_ETHERNET_CARRIER_ON:
#if defined(CONFIG_NET_DHCPV4)
		/* Restarting on carrier-on guarantees DHCP when cable is inserted later. */
		net_dhcpv4_restart(iface);
#endif
		break;
	default:
		break;
	}
}

int main(void)
{
	int ret;
	bool led_on = true;

	target_iface = net_if_get_default();
	atomic_set(&net_ready, 0);
	atomic_set(&update_pending, 0);
	atomic_set(&polling_started, 0);
	atomic_set(&waiting_for_ipv4_logged, 0);

	if (IS_ENABLED(CONFIG_BOOTLOADER_MCUBOOT)) {
		bool confirmed = boot_is_img_confirmed();
		int swap_type = mcuboot_swap_type();

		atomic_set(&image_confirmed, confirmed ? 1 : 0);
		LOG_INF("Current image is %sconfirmed", confirmed ? "" : "not ");

		if (swap_type > 0 && swap_type != BOOT_SWAP_TYPE_NONE) {
			atomic_set(&update_pending, 1);
			LOG_INF("MCUboot swap already pending at boot (type=%d)", swap_type);
		}
	} else {
		atomic_set(&image_confirmed, 1);
	}

	if (!gpio_is_ready_dt(&led)) {
		LOG_ERR("LED device is not ready");
		return 0;
	}

	ret = gpio_pin_configure_dt(&led, GPIO_OUTPUT_ACTIVE);
	if (ret < 0) {
		LOG_ERR("Cannot configure LED GPIO (%d)", ret);
		return 0;
	}

	net_mgmt_init_event_callback(&net_if_cb, on_net_event, NET_IF_EVENT_MASK);
	net_mgmt_add_event_callback(&net_if_cb);
	net_mgmt_init_event_callback(&net_ipv4_cb, on_net_event, NET_IPV4_EVENT_MASK);
	net_mgmt_add_event_callback(&net_ipv4_cb);
	net_mgmt_init_event_callback(&net_eth_cb, on_net_event, NET_ETH_EVENT_MASK);
	net_mgmt_add_event_callback(&net_eth_cb);

	if (target_iface == NULL) {
		LOG_WRN("No default network interface found");
	} else {
		ret = net_if_up(target_iface);
		if (ret == -EALREADY) {
			LOG_INF("Network interface %d (%s) already up",
				net_if_get_by_iface(target_iface),
				net_if_get_device(target_iface)->name);
			set_net_ready_state(iface_has_ipv4_addr(target_iface), "IF_ALREADY_UP");
			confirm_running_image_once();
		} else if (ret < 0) {
			LOG_WRN("net_if_up failed (%d)", ret);
		}

		net_dhcpv4_start(target_iface);
		LOG_INF("DHCPv4 client started on iface %d (%s)",
			net_if_get_by_iface(target_iface),
			net_if_get_device(target_iface)->name);
	}

	LOG_INF("Application started");

	while (1) {
		ret = gpio_pin_toggle_dt(&led);
		if (ret < 0) {
			LOG_ERR("Cannot toggle LED (%d)", ret);
			return 0;
		}

		led_on = !led_on;
		LOG_DBG("LED state: %s", led_on ? "ON" : "OFF");
		k_sleep(BLINK_PERIOD);
	}
}
