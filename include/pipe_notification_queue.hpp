#pragma once
#include <memory>
#include <event2/util.h>

#include "queue.hpp"

// We need a thread safe mechanism of talking to libevent
class NotificationPipeTransmitter
{
public:
	explicit NotificationPipeTransmitter(evutil_socket_t notification_pipe) : notification_pipe_(notification_pipe) {}

	bool notify(const char cmd)
	{
		std::lock_guard<std::mutex> lock(mutex_);
		if (const int num_tx = send(notification_pipe_, &cmd, 1, 0); num_tx > 0)
		{
			LOG_DEBUG("Sent command: " << cmd );
			last_notification_sent_ = std::chrono::high_resolution_clock::now();
			last_notification_cmd_ = cmd;
			return true;
		}
		else
		{
			// This regularly occurs when we have a very fast rate of data to handle and the notification queue becomes full
			LOG_TRACE("Failed to send command to the notification pipe");
			return false;
		}
	}
private:
	std::chrono::high_resolution_clock::time_point last_notification_sent_ {std::chrono::high_resolution_clock::now()};
	char last_notification_cmd_ {0x00};
	evutil_socket_t notification_pipe_ {-1};
	std::mutex mutex_;
};

/**
 * Provide a queue variant that will allow us to notify RMQ via a pipe notification when
 * we add new data to the queue.
 *
 * Typical usage:
 * - Declare the queue
 * - Add data to the queue
 *
 * @tparam Typ
 */
template<typename Typ>
class PipeNotificationQueue final : public Queue<Typ> {
public:
	explicit PipeNotificationQueue(size_t max_size
	                               , QueueOverflowPolicy default_overflow_policy = QueueOverflowPolicy::WAIT
	                               , const std::string& name = "queue") : Queue<Typ>(max_size, default_overflow_policy, name)
	{}

	void setupNotificationPipe(const char notification_cmd, const std::shared_ptr<NotificationPipeTransmitter>  &notification_pipe)
	{
		LOG_DEBUG("Setting up notification pipe for command: " << notification_cmd);
		notification_cmd_ = notification_cmd;
		notification_pipe_ = notification_pipe;
	}

	void push(const Typ& obj) override
	{
		LOG_DEBUG("Pushing object to queue: " << obj);
		Queue<Typ>::push(obj);
		notify();
	}

	void push(Typ&& obj) override
	{
		LOG_DEBUG("Pushing object to queue: " << obj);
		Queue<Typ>::push(std::move(obj));
		notify();
	}

	void notify() const
	{
		if (notification_pipe_)
		{
			notification_pipe_->notify(notification_cmd_);
		}
	}


private:
	char notification_cmd_ {0x00};
	std::shared_ptr<NotificationPipeTransmitter> notification_pipe_ {nullptr};
};
