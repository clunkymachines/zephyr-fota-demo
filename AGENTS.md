This project is a demo of firmware update over network for Zehpyr based boards.

The codce is generic and contains some customization capabilities using overlays for the different boards.

It uses the zephyr installation in ~/sandbox/zephyrproject/zephyr

The project must use sysbuild, to have mcuboot to handle partition switching

The application will: 
 - blink the LED (like the blinky sample)
 - expose shell on the console UART
 - display logs using the shell backend
 - initialize the IPv4 networking using the DHCP client over the default ethernet port
 - show in the console the different networking events related to ethernet/IPv4
 - the device will connect to a MQTT broker on IP 10.42.0.1, with the client ID "demo001" (once the device got a IPv4 from DHCP)
 - once connected to MQTT the device will:
    - subscribe to the topic "device/demo001/task", decode the CBOR message '{"task": [type of the task], "param" : [ a 256 bytes string] }' and log it
    - if the task is "fota", then use the param as an http url and try to download the binary image and flash the update partition
    - publish every 5 second using QOS0 on the topic "device/demo001/data" a CBOR message with the equivalent of the json representation '{"uptime":[uptime in seconds]}'
 - the url will be http://vrm.free.fr/demo.bin
 - the URL is checked every 10 seconds, if it's 200, then download the file and write it in the second partition
 - on boot if the device sucesfully get a IPv4 address mark the partition as 'tested'
