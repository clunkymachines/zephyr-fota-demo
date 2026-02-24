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
 - the firmware will not use MCUmgr to update but a simple URL polling to download a file over HTTP
 - the url will be http://vrm.free.fr/demo.bin
 - the URL is checked every 10 seconds, if it's 200, then download the file and write it in the second partition
 - on boot if the device sucesfully get a IPv4 address mark the partition as 'tested'
