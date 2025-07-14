#pragma once
#include <memory>
#include <mutex>
#include <queue>

namespace rmq {
/**
 * This class handles the data into and out from channels
 * @tparam MessageType
 */
class MyAmqpTxChannelDataHandler {
public:
	using MessageType = std::shared_ptr<std::vector<char>>;
	void pushMessage(MessageType message)
	{
		std::lock_guard lock(mutex_);
		queue_.push(std::move(message));
	}

	// Get the next message from the queue without removing it
	MessageType front()
	{
		std::lock_guard lock(mutex_);
		return queue_.front();
	}

	void popMessage()
	{
		std::lock_guard lock(mutex_);
		queue_.pop();
	}

	bool isEmpty()
	{
		std::lock_guard lock(mutex_);
		return queue_.empty();
	}

	size_t size()
	{
		std::lock_guard lock(mutex_);
		return queue_.size();
	}

private:
	std::mutex mutex_;
	std::queue<MessageType> queue_;
};

} // rmq
