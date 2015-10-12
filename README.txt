Instructions:

1.	cd to each server directory and execute the ./start.sh script
2.	Wait for 10-15 seconds between start of each server. This delay is required for stabilizing the chord ring.
3.	Once the nodes have joined the ring, you can cd to Client directory and execute the ./start.sh script
4.	The config.json file in client folder holds the port number of the server to be contacted.
5.	The commands.txt holds the instructions to be executed
6.	Use the 'exit' command to shutdown the client.
7.	Use 'shutdown()' command to shutdown the server.
8.	Once a server is shut down, change the port number in config file of client to connect to new server.

