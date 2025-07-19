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
		is_ready_.store(true);
	}

	bool isReady() const
	{
		return is_ready_.load() ;
	}

private:
	std::atomic<bool> is_ready_{false};
};


class MyAmqpControllerNoChannel
{
public:
	MyAmqpControllerNoChannel()
	{

	}
	~MyAmqpControllerNoChannel()
	{
	}

	static void on_timeout(evutil_socket_t fd, short events, void* arg)
	{
		std::cout << "on_timeout" << std::endl;
		MyAmqpControllerNoChannel* self = static_cast<MyAmqpControllerNoChannel*>(arg);
		if ( self->request_close_.load() )
		{
			std::cout << "on_timeout - planning to close now" << std::endl;
			self->connection->close();
			event_del(self->timeout_event_);
			self->request_close_.store(false);
		}
	}

	void nudge_event_loop()
	{
		int num_events = event_base_get_num_events(evbase, EVENT_BASE_COUNT_ADDED);
		std::string msg = "Number of active events is: " + std::to_string(num_events);
		event_base_loopcontinue(evbase);
	}

	void run()
	{
		evbase = event_base_new();
		handler = std::make_unique<MyNoChannelLibEventHandler>(evbase);
		connection = std::make_unique<AMQP::TcpConnection>(handler.get(), AMQP::Address("amqp://guest:guest@localhost/"));

		// Create a new event (EV_PERSIST for repeating)
		struct timeval tv = {0, 100000}; // 0 seconds, 100000 microseconds = 100ms

		timeout_event_ = event_new(evbase, -1, EV_PERSIST, on_timeout, this);
		if (!timeout_event_) {
			connection.reset();
			handler.reset();
			event_base_free(evbase);
			throw std::runtime_error("Could not create timeout event.");
		}

		// Add the event with the initial timer
		event_add(timeout_event_, &tv);
		event_base_dispatch(evbase);
		event_free(timeout_event_);
		event_base_free(evbase);
	}

	bool isConnectionReady() const
	{
		if (!handler) { return false;}
		return handler->isReady();
	}

	void close()
	{
		event_base_dump_events(evbase, stderr);
		request_close_.store(true);
	}

	bool isRequestClose()
	{
		return request_close_.load();
	}

private:
	std::atomic<bool> request_close_ {false};

	struct event_base *evbase;
	std::unique_ptr<MyNoChannelLibEventHandler> handler;
	std::unique_ptr<AMQP::TcpConnection> connection;
	struct event* timeout_event_;
};


