#pragma once
#include <iostream>
#include <memory>

#include <amqpcpp.h>
#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/table.h>

using namespace AMQP;

class MyAmqpControllerExample {
public:
	MyAmqpControllerExample()
	{
	}
	~MyAmqpControllerExample()
	{

	}

	void run()
	{
		evbase = event_base_new();
		handler = std::make_unique<AMQP::LibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));

		// we need a channel too
		AMQP::TcpChannel channel(connection.get());

		// create a temporary queue
		channel.declareQueue(AMQP::exclusive).onSuccess([this](const std::string &name, uint32_t messagecount, uint32_t consumercount) {

			// report the name of the temporary queue
			std::cout << "declared queue " << name << std::endl;

			// now we can close the connection
			connection->close();
		});

		event_base_dispatch(evbase);
		event_base_free(evbase);
	}

private:
	struct event_base *evbase;
	std::unique_ptr<AMQP::LibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;
};
