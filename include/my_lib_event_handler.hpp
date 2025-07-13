#pragma once
#include <iostream>
#include <ostream>

#include "amqpcpp/libevent.h"

class MyLibEventHandler : public AMQP::LibEventHandler
{
public:
	MyLibEventHandler(struct event_base *evbase) : LibEventHandler(evbase)
	{

	}

	virtual ~MyLibEventHandler()
	{

	}

	void onReady(AMQP::TcpConnection *connection) override
	{
		std::cout << "onReady - planning to close now" << std::endl;
		is_ready_ = true;
	}

	bool isReady()
	{
		return is_ready_;
	}

private:
	std::atomic<bool> is_ready_ {false};

};
