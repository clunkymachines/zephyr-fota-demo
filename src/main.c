/*
 * SPDX-License-Identifier: Apache-2.0
 */

#include <zephyr/kernel.h>
#include <zephyr/drivers/gpio.h>
#include <zephyr/logging/log.h>
#include <zephyr/logging/log_ctrl.h>
#include <zephyr/sys/atomic.h>
#include <zephyr/sys/reboot.h>
#include <zephyr/sys/util.h>
#include <zephyr/net/socket.h>
#include <zephyr/net/http/client.h>
#include <zephyr/net/net_if.h>
#include <zephyr/net/dhcpv4.h>
#include <zephyr/net/ethernet_mgmt.h>
#include <zephyr/net/net_event.h>
#include <zephyr/net/net_mgmt.h>
#include <zephyr/net/mqtt.h>
#include <zephyr/dfu/flash_img.h>
#include <zephyr/dfu/mcuboot.h>
#include <zcbor_decode.h>
#include <zcbor_encode.h>
#include <errno.h>
#include <string.h>

LOG_MODULE_REGISTER(app, LOG_LEVEL_INF);

#define LED0_NODE DT_ALIAS(led0)
#define BLINK_PERIOD K_MSEC(100)

#define FOTA_THREAD_STACK_SIZE 4096
#define FOTA_THREAD_PRIORITY 7

#define MQTT_THREAD_STACK_SIZE 4608
#define MQTT_THREAD_PRIORITY 7
#define MQTT_BUFFER_SIZE 1024
#define MQTT_BROKER_ADDR "10.42.0.1"
#define MQTT_BROKER_PORT 1883
#define MQTT_CLIENT_ID "demo001"
#define MQTT_TOPIC_TASK "device/demo001/task"
#define MQTT_TOPIC_DATA "device/demo001/data"
#define MQTT_RECONNECT_DELAY K_SECONDS(2)
#define MQTT_SERVICE_IDLE_SLICE_MS 1000
#define MQTT_CONNECT_TIMEOUT_MS 5000
#define MQTT_PUBLISH_PERIOD_MS 5000
#define MQTT_TASK_PAYLOAD_BUF_SIZE 320
#define MQTT_DATA_PAYLOAD_BUF_SIZE 64

#define FOTA_URL_MAX_LEN 256
#define HTTP_DEFAULT_PORT "80"
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
static atomic_t mqtt_connected;
static atomic_t mqtt_subscribed;
static atomic_t mqtt_waiting_for_ipv4_logged;
static atomic_t fota_pending;
static atomic_t fota_ongoing;

static struct mqtt_client mqtt_client_ctx;
static struct sockaddr_storage mqtt_broker;
static struct zsock_pollfd mqtt_fds[1];
static int mqtt_nfds;
static uint8_t mqtt_rx_buffer[MQTT_BUFFER_SIZE];
static uint8_t mqtt_tx_buffer[MQTT_BUFFER_SIZE];
static uint16_t mqtt_message_id = 1;

K_SEM_DEFINE(fota_request_sem, 0, 1);
K_MUTEX_DEFINE(fota_url_mutex);
static char pending_fota_url[FOTA_URL_MAX_LEN + 1];

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

struct parsed_http_url {
	char host[128];
	char port[6];
	char path[FOTA_URL_MAX_LEN + 1];
};

struct mqtt_task_message {
	char task[32];
	char param[FOTA_URL_MAX_LEN + 1];
	bool has_task;
	bool has_param;
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
			LOG_INF("No MCUboot swap pending anymore");
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
	atomic_set(&update_pending, 0);
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

static int parse_http_url(const char *url, struct parsed_http_url *parsed)
{
	const char *prefix = "http://";
	const char *rest;
	const char *slash;
	size_t hostport_len;
	char hostport[sizeof(parsed->host) + sizeof(parsed->port) + 2];
	char *port_sep;

	if (url == NULL || parsed == NULL) {
		return -EINVAL;
	}

	if (strncmp(url, prefix, strlen(prefix)) != 0) {
		return -EINVAL;
	}

	rest = url + strlen(prefix);
	slash = strchr(rest, '/');
	hostport_len = slash ? (size_t)(slash - rest) : strlen(rest);
	if (hostport_len == 0 || hostport_len >= sizeof(hostport)) {
		return -EINVAL;
	}

	memcpy(hostport, rest, hostport_len);
	hostport[hostport_len] = '\0';
	port_sep = strrchr(hostport, ':');

	if (port_sep != NULL) {
		size_t host_len = (size_t)(port_sep - hostport);
		size_t port_len = strlen(port_sep + 1);

		if (host_len == 0 || host_len >= sizeof(parsed->host)) {
			return -EINVAL;
		}
		if (port_len == 0 || port_len >= sizeof(parsed->port)) {
			return -EINVAL;
		}

		memcpy(parsed->host, hostport, host_len);
		parsed->host[host_len] = '\0';
		strcpy(parsed->port, port_sep + 1);
	} else {
		if (hostport_len >= sizeof(parsed->host)) {
			return -EINVAL;
		}
		strcpy(parsed->host, hostport);
		strcpy(parsed->port, HTTP_DEFAULT_PORT);
	}

	if (slash == NULL) {
		strcpy(parsed->path, "/");
	} else {
		size_t path_len = strlen(slash);

		if (path_len == 0 || path_len >= sizeof(parsed->path)) {
			return -EINVAL;
		}
		strcpy(parsed->path, slash);
	}

	return 0;
}

static int download_update_from_url(const char *url)
{
	static const char *headers[] = {
		"Connection: close\r\n",
		NULL,
	};
	uint8_t recv_buf[UPDATE_RECV_BUF_SIZE];
	struct parsed_http_url parsed = {0};
	struct update_download_context dl_ctx = {0};
	struct http_request req = {0};
	int sock;
	int ret;

	ret = parse_http_url(url, &parsed);
	if (ret < 0) {
		LOG_ERR("Invalid FOTA URL: %s", url);
		return ret;
	}

	sock = connect_http_socket(parsed.host, parsed.port);
	if (sock < 0) {
		return sock;
	}

	LOG_INF("Downloading FOTA URL: %s", url);

	req.method = HTTP_GET;
	req.url = parsed.path;
	req.host = parsed.host;
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
		LOG_INF("FOTA URL not available (HTTP %d)", dl_ctx.status_code);
		return -ENOENT;
	}

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

static bool mqtt_topic_equals(const struct mqtt_utf8 *topic, const char *expected)
{
	size_t expected_len = strlen(expected);

	return topic->size == expected_len && memcmp(topic->utf8, expected, expected_len) == 0;
}

static uint16_t next_mqtt_message_id(void)
{
	mqtt_message_id++;
	if (mqtt_message_id == 0U) {
		mqtt_message_id = 1U;
	}

	return mqtt_message_id;
}

static void mqtt_prepare_fds(struct mqtt_client *client)
{
	if (client->transport.type != MQTT_TRANSPORT_NON_SECURE) {
		mqtt_nfds = 0;
		return;
	}

	mqtt_fds[0].fd = client->transport.tcp.sock;
	mqtt_fds[0].events = ZSOCK_POLLIN;
	mqtt_nfds = 1;
}

static void mqtt_clear_fds(void)
{
	mqtt_nfds = 0;
}

static int mqtt_socket_wait(int timeout_ms)
{
	if (mqtt_nfds == 0) {
		k_sleep(K_MSEC(timeout_ms));
		return 0;
	}

	return zsock_poll(mqtt_fds, mqtt_nfds, timeout_ms);
}

static int mqtt_publish_uptime(void)
{
	uint8_t payload[MQTT_DATA_PAYLOAD_BUF_SIZE];
	struct mqtt_publish_param param = {0};
	int64_t uptime_sec = k_uptime_get() / MSEC_PER_SEC;
	size_t encoded_len;
	bool ok;

	ZCBOR_STATE_E(zse, 2, payload, sizeof(payload), 1);

	ok = zcbor_map_start_encode(zse, 1) &&
	     zcbor_tstr_put_lit(zse, "uptime") &&
	     zcbor_int64_put(zse, uptime_sec) &&
	     zcbor_map_end_encode(zse, 1);
	if (!ok) {
		LOG_ERR("Failed to encode uptime CBOR");
		return -EMSGSIZE;
	}

	encoded_len = (size_t)(zse->payload - payload);

	param.message.topic.topic.utf8 = (uint8_t *)MQTT_TOPIC_DATA;
	param.message.topic.topic.size = strlen(MQTT_TOPIC_DATA);
	param.message.topic.qos = MQTT_QOS_0_AT_MOST_ONCE;
	param.message.payload.data = payload;
	param.message.payload.len = encoded_len;
	param.message_id = next_mqtt_message_id();
	param.dup_flag = 0U;
	param.retain_flag = 0U;

	return mqtt_publish(&mqtt_client_ctx, &param);
}

static int mqtt_subscribe_task_topic(struct mqtt_client *client)
{
	struct mqtt_topic topic = {
		.topic = {
			.utf8 = (uint8_t *)MQTT_TOPIC_TASK,
			.size = strlen(MQTT_TOPIC_TASK),
		},
		.qos = MQTT_QOS_0_AT_MOST_ONCE,
	};
	const struct mqtt_subscription_list sub = {
		.list = &topic,
		.list_count = 1U,
		.message_id = next_mqtt_message_id(),
	};

	return mqtt_subscribe(client, &sub);
}

static int mqtt_drain_publish_payload(struct mqtt_client *client, size_t len)
{
	uint8_t discard[64];
	size_t remaining = len;

	while (remaining > 0U) {
		size_t chunk = MIN(remaining, sizeof(discard));
		int ret = mqtt_read_publish_payload(client, discard, chunk);

		if (ret < 0) {
			return ret;
		}
		if (ret == 0) {
			return -EIO;
		}

		remaining -= (size_t)ret;
	}

	return 0;
}

static int mqtt_read_publish_payload_all(struct mqtt_client *client, uint8_t *buf, size_t len)
{
	size_t total = 0;

	while (total < len) {
		size_t chunk = len - total;
		int ret = mqtt_read_publish_payload(client, buf + total, chunk);

		if (ret < 0) {
			return ret;
		}
		if (ret == 0) {
			return -EIO;
		}

		total += (size_t)ret;
	}

	return (int)total;
}

static int decode_task_cbor(const uint8_t *payload, size_t payload_len, struct mqtt_task_message *msg)
{
	if (payload == NULL || msg == NULL) {
		return -EINVAL;
	}

	ZCBOR_STATE_D(zsd, 4, payload, payload_len, 1, 0);

	if (!zcbor_map_start_decode(zsd)) {
		return -EBADMSG;
	}

	while (!zcbor_array_at_end(zsd)) {
		struct zcbor_string key;

		if (!zcbor_tstr_decode(zsd, &key)) {
			return -EBADMSG;
		}

		if (key.len == 4U && memcmp(key.value, "task", 4) == 0) {
			struct zcbor_string task;

			if (!zcbor_tstr_decode(zsd, &task)) {
				return -EBADMSG;
			}
			if (task.len == 0U || task.len >= sizeof(msg->task)) {
				return -EMSGSIZE;
			}

			memcpy(msg->task, task.value, task.len);
			msg->task[task.len] = '\0';
			msg->has_task = true;
		} else if (key.len == 5U && memcmp(key.value, "param", 5) == 0) {
			struct zcbor_string param;

			if (!zcbor_tstr_decode(zsd, &param)) {
				return -EBADMSG;
			}
			if (param.len > FOTA_URL_MAX_LEN) {
				return -EMSGSIZE;
			}

			memcpy(msg->param, param.value, param.len);
			msg->param[param.len] = '\0';
			msg->has_param = true;
		} else {
			if (!zcbor_any_skip(zsd, NULL)) {
				return -EBADMSG;
			}
		}
	}

	if (!zcbor_map_end_decode(zsd)) {
		return -EBADMSG;
	}

	if (!msg->has_task) {
		return -EBADMSG;
	}

	return 0;
}

static void schedule_fota_from_url(const char *url)
{
	size_t url_len = strlen(url);

	if (strncmp(url, "http://", strlen("http://")) != 0) {
		LOG_WRN("Ignoring FOTA task: URL must start with http://");
		return;
	}

	if (url_len == 0U || url_len > FOTA_URL_MAX_LEN) {
		LOG_WRN("Ignoring FOTA task: URL length invalid (%u)", (unsigned int)url_len);
		return;
	}

	if (atomic_get(&fota_ongoing) || atomic_get(&fota_pending)) {
		LOG_WRN("Ignoring FOTA task: update is already ongoing");
		return;
	}

	if (atomic_get(&update_pending) || is_upgrade_already_pending()) {
		LOG_WRN("Ignoring FOTA task: MCUboot swap already pending");
		return;
	}

	k_mutex_lock(&fota_url_mutex, K_FOREVER);
	strcpy(pending_fota_url, url);
	atomic_set(&fota_pending, 1);
	k_mutex_unlock(&fota_url_mutex);

	k_sem_give(&fota_request_sem);
	LOG_INF("FOTA task scheduled: %s", url);
}

static void handle_mqtt_publish(struct mqtt_client *client, const struct mqtt_publish_param *pub)
{
	uint8_t payload[MQTT_TASK_PAYLOAD_BUF_SIZE];
	size_t payload_len = pub->message.payload.len;
	struct mqtt_task_message msg = {0};
	int ret;

	if (!mqtt_topic_equals(&pub->message.topic.topic, MQTT_TOPIC_TASK)) {
		(void)mqtt_drain_publish_payload(client, payload_len);
		return;
	}

	if (payload_len > sizeof(payload)) {
		LOG_WRN("Task payload too large (%u bytes)", (unsigned int)payload_len);
		(void)mqtt_drain_publish_payload(client, payload_len);
		return;
	}

	ret = mqtt_read_publish_payload_all(client, payload, payload_len);
	if (ret < 0) {
		LOG_ERR("Failed to read task payload (%d)", ret);
		return;
	}

	ret = decode_task_cbor(payload, payload_len, &msg);
	if (ret < 0) {
		LOG_WRN("Failed to decode CBOR task (%d)", ret);
		return;
	}

	LOG_INF("MQTT task received: task='%s' param='%s'", msg.task, msg.has_param ? msg.param : "");

	if (strcmp(msg.task, "fota") != 0) {
		LOG_INF("Unsupported task type '%s'", msg.task);
		return;
	}

	if (!msg.has_param || msg.param[0] == '\0') {
		LOG_WRN("Ignoring FOTA task: missing URL parameter");
		return;
	}

	schedule_fota_from_url(msg.param);
}

static void mqtt_evt_handler(struct mqtt_client *const client, const struct mqtt_evt *evt)
{
	switch (evt->type) {
	case MQTT_EVT_CONNACK:
		if (evt->result != 0) {
			LOG_ERR("MQTT connect failed (%d)", evt->result);
			break;
		}

		atomic_set(&mqtt_connected, 1);
		atomic_set(&mqtt_subscribed, 0);
		LOG_INF("MQTT connected to %s:%d as %s", MQTT_BROKER_ADDR, MQTT_BROKER_PORT,
			MQTT_CLIENT_ID);

		int sub_ret = mqtt_subscribe_task_topic(client);
		if (sub_ret < 0) {
			LOG_ERR("MQTT subscribe failed (%d)", sub_ret);
		}
		break;

	case MQTT_EVT_DISCONNECT:
		LOG_INF("MQTT disconnected (%d)", evt->result);
		atomic_set(&mqtt_connected, 0);
		atomic_set(&mqtt_subscribed, 0);
		mqtt_clear_fds();
		break;

	case MQTT_EVT_SUBACK:
		if (evt->result != 0) {
			LOG_ERR("MQTT SUBACK error (%d)", evt->result);
			break;
		}
		atomic_set(&mqtt_subscribed, 1);
		LOG_INF("Subscribed to topic %s", MQTT_TOPIC_TASK);
		break;

	case MQTT_EVT_PUBLISH:
		if (evt->param.publish.message.topic.qos == MQTT_QOS_1_AT_LEAST_ONCE) {
			const struct mqtt_puback_param ack = {
				.message_id = evt->param.publish.message_id,
			};
			(void)mqtt_publish_qos1_ack(client, &ack);
		} else if (evt->param.publish.message.topic.qos == MQTT_QOS_2_EXACTLY_ONCE) {
			const struct mqtt_pubrec_param rec = {
				.message_id = evt->param.publish.message_id,
			};
			(void)mqtt_publish_qos2_receive(client, &rec);
		}

		handle_mqtt_publish(client, &evt->param.publish);
		break;

	case MQTT_EVT_PINGRESP:
		LOG_DBG("MQTT ping response");
		break;

	default:
		break;
	}
}

static int mqtt_broker_init(void)
{
	struct sockaddr_in *broker4 = (struct sockaddr_in *)&mqtt_broker;
	int ret;

	memset(&mqtt_broker, 0, sizeof(mqtt_broker));
	broker4->sin_family = AF_INET;
	broker4->sin_port = htons(MQTT_BROKER_PORT);

	ret = zsock_inet_pton(AF_INET, MQTT_BROKER_ADDR, &broker4->sin_addr);
	if (ret != 1) {
		return -EINVAL;
	}

	return 0;
}

static void mqtt_client_prepare(struct mqtt_client *client)
{
	mqtt_client_init(client);

	client->broker = &mqtt_broker;
	client->evt_cb = mqtt_evt_handler;
	client->client_id.utf8 = (uint8_t *)MQTT_CLIENT_ID;
	client->client_id.size = strlen(MQTT_CLIENT_ID);
	client->password = NULL;
	client->user_name = NULL;
	client->protocol_version = MQTT_VERSION_3_1_1;
	client->transport.type = MQTT_TRANSPORT_NON_SECURE;

	client->rx_buf = mqtt_rx_buffer;
	client->rx_buf_size = sizeof(mqtt_rx_buffer);
	client->tx_buf = mqtt_tx_buffer;
	client->tx_buf_size = sizeof(mqtt_tx_buffer);
}

static void mqtt_abort_and_reset(void)
{
	(void)mqtt_abort(&mqtt_client_ctx);
	atomic_set(&mqtt_connected, 0);
	atomic_set(&mqtt_subscribed, 0);
	mqtt_clear_fds();
}

static int mqtt_connect_once(void)
{
	int ret;
	int64_t deadline;

	ret = mqtt_broker_init();
	if (ret < 0) {
		LOG_ERR("Invalid MQTT broker address (%d)", ret);
		return ret;
	}

	mqtt_client_prepare(&mqtt_client_ctx);

	ret = mqtt_connect(&mqtt_client_ctx);
	if (ret < 0) {
		LOG_WRN("mqtt_connect failed (%d)", ret);
		return ret;
	}

	mqtt_prepare_fds(&mqtt_client_ctx);
	deadline = k_uptime_get() + MQTT_CONNECT_TIMEOUT_MS;

	while (!atomic_get(&mqtt_connected) && k_uptime_get() < deadline) {
		int remaining = (int)(deadline - k_uptime_get());
		int poll_ret = mqtt_socket_wait(MAX(remaining, 0));

		if (poll_ret < 0) {
			LOG_WRN("MQTT connect poll failed (%d)", poll_ret);
			mqtt_abort_and_reset();
			return poll_ret;
		}

		if (poll_ret > 0) {
			ret = mqtt_input(&mqtt_client_ctx);
			if (ret < 0) {
				LOG_WRN("mqtt_input failed during connect (%d)", ret);
				mqtt_abort_and_reset();
				return ret;
			}
		}
	}

	if (!atomic_get(&mqtt_connected)) {
		LOG_WRN("MQTT connect timeout");
		mqtt_abort_and_reset();
		return -ETIMEDOUT;
	}

	return 0;
}

static void mqtt_thread_fn(void *arg1, void *arg2, void *arg3)
{
	int64_t next_publish = 0;

	ARG_UNUSED(arg1);
	ARG_UNUSED(arg2);
	ARG_UNUSED(arg3);

	while (1) {
		if (!atomic_get(&net_ready)) {
			if (!atomic_get(&mqtt_waiting_for_ipv4_logged)) {
				atomic_set(&mqtt_waiting_for_ipv4_logged, 1);
				LOG_INF("MQTT paused: waiting for IPv4 assignment");
			}

			if (atomic_get(&mqtt_connected) || mqtt_nfds > 0) {
				mqtt_abort_and_reset();
			}

			k_sleep(K_SECONDS(1));
			continue;
		}

		if (atomic_get(&mqtt_waiting_for_ipv4_logged)) {
			atomic_set(&mqtt_waiting_for_ipv4_logged, 0);
			LOG_INF("IPv4 ready, MQTT service active");
		}

		if (!atomic_get(&mqtt_connected)) {
			int ret = mqtt_connect_once();

			if (ret < 0) {
				k_sleep(MQTT_RECONNECT_DELAY);
				continue;
			}

			next_publish = k_uptime_get() + MQTT_PUBLISH_PERIOD_MS;
		}

		int timeout_ms = MQTT_SERVICE_IDLE_SLICE_MS;
		int keepalive_left = mqtt_keepalive_time_left(&mqtt_client_ctx);
		int64_t now = k_uptime_get();
		int64_t to_publish = next_publish - now;

		if (keepalive_left > 0 && keepalive_left < timeout_ms) {
			timeout_ms = keepalive_left;
		}

		if (to_publish < timeout_ms) {
			timeout_ms = MAX(0, (int)to_publish);
		}

		int poll_ret = mqtt_socket_wait(timeout_ms);
		if (poll_ret < 0) {
			LOG_WRN("MQTT socket wait failed (%d)", poll_ret);
			mqtt_abort_and_reset();
			k_sleep(MQTT_RECONNECT_DELAY);
			continue;
		}

		if (poll_ret > 0 && atomic_get(&mqtt_connected)) {
			int ret = mqtt_input(&mqtt_client_ctx);

			if (ret < 0) {
				LOG_WRN("mqtt_input failed (%d)", ret);
				mqtt_abort_and_reset();
				k_sleep(MQTT_RECONNECT_DELAY);
				continue;
			}
		}

		if (atomic_get(&mqtt_connected)) {
			int ret = mqtt_live(&mqtt_client_ctx);

			if (ret != 0 && ret != -EAGAIN) {
				LOG_WRN("mqtt_live failed (%d)", ret);
				mqtt_abort_and_reset();
				k_sleep(MQTT_RECONNECT_DELAY);
				continue;
			}
		}

		now = k_uptime_get();
		if (atomic_get(&mqtt_connected) && now >= next_publish) {
			int ret = mqtt_publish_uptime();

			if (ret < 0) {
				LOG_WRN("Failed to publish uptime (%d)", ret);
			}

			next_publish = now + MQTT_PUBLISH_PERIOD_MS;
		}
	}
}

static void fota_thread_fn(void *arg1, void *arg2, void *arg3)
{
	char url[FOTA_URL_MAX_LEN + 1];

	ARG_UNUSED(arg1);
	ARG_UNUSED(arg2);
	ARG_UNUSED(arg3);

	while (1) {
		k_sem_take(&fota_request_sem, K_FOREVER);

		if (!atomic_cas(&fota_pending, 1, 0)) {
			continue;
		}

		if (!atomic_get(&net_ready)) {
			LOG_WRN("Ignoring queued FOTA task: network is not ready");
			continue;
		}

		if (atomic_get(&fota_ongoing)) {
			LOG_WRN("Ignoring queued FOTA task: update is already running");
			continue;
		}

		if (atomic_get(&update_pending) || is_upgrade_already_pending()) {
			LOG_WRN("Ignoring queued FOTA task: MCUboot swap already pending");
			continue;
		}

		k_mutex_lock(&fota_url_mutex, K_FOREVER);
		strcpy(url, pending_fota_url);
		k_mutex_unlock(&fota_url_mutex);

		atomic_set(&fota_ongoing, 1);
		int ret = download_update_from_url(url);

		if (ret < 0) {
			LOG_ERR("FOTA from '%s' failed (%d)", url, ret);
		}

		atomic_set(&fota_ongoing, 0);
	}
}

K_THREAD_DEFINE(mqtt_thread, MQTT_THREAD_STACK_SIZE, mqtt_thread_fn, NULL, NULL, NULL,
		MQTT_THREAD_PRIORITY, 0, 0);
K_THREAD_DEFINE(fota_thread, FOTA_THREAD_STACK_SIZE, fota_thread_fn, NULL, NULL, NULL,
		FOTA_THREAD_PRIORITY, 0, 0);

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
	atomic_set(&mqtt_connected, 0);
	atomic_set(&mqtt_subscribed, 0);
	atomic_set(&mqtt_waiting_for_ipv4_logged, 0);
	atomic_set(&fota_pending, 0);
	atomic_set(&fota_ongoing, 0);

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
