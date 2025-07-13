#pragma once
#include <iostream>
#include <memory>

#include <amqpcpp/address.h>
#include <amqpcpp/libevent.h>


using namespace AMQP;

class MyNoChannelLibEventHandler : public AMQP::LibEventHandler
{
public:
	MyNoChannelLibEventHandler(struct event_base *evbase) : LibEventHandler(evbase)
	{

	}

	virtual ~MyNoChannelLibEventHandler()
	{

	}

	void onReady(AMQP::TcpConnection *connection) override
	{
		std::cout << "onReady - planning to close now" << std::endl;
		connection->close();
	}


};


class MyAmqpControllerNoChannel
{
public:
	MyAmqpControllerNoChannel()
	{
		evbase = event_base_new();
		handler = std::make_unique<MyNoChannelLibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));
	}
	~MyAmqpControllerNoChannel()
	{

	}

	void run()
	{
		event_base_dispatch(evbase);
		event_base_free(evbase);
	}

private:
	struct event_base *evbase;
	std::unique_ptr<MyNoChannelLibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;
};


