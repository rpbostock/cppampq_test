#pragma once

#include "my_amqp_controller.hpp"

class RxClientWrapper
{
public:
	explicit RxClientWrapper(const std::string &channel_name
		, const rmq::MyRxDataQueuePtr& queue
		, const std::shared_ptr<rmq::ChannelListener> &listener
		, const std::function<void(const rmq::IMessageAck& ack)> &ack_fn) :
	channel_name_(channel_name), queue_(queue), listener_(listener), acknowledge_fn_(ack_fn)
	{}

	std::string getChannelName() const { return channel_name_; }
	std::shared_ptr<rmq::ChannelListener> getListener() const  { return listener_; }
	rmq::MyRxDataQueuePtr getQueue() const { return queue_; }
	void acknowledge(const rmq::IMessageAck& ack) const { acknowledge_fn_(ack); }

	// Declare the operators as friends but define them outside
	friend bool operator<(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator<=(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator>(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator>=(const RxClientWrapper& lhs, const RxClientWrapper& rhs);

private:
	std::string channel_name_;
	std::shared_ptr<rmq::ChannelListener> listener_;
	rmq::MyRxDataQueuePtr queue_;
	std::function<void(const rmq::IMessageAck& ack)> acknowledge_fn_;
};


inline bool operator<(const RxClientWrapper &lhs, const RxClientWrapper &rhs)
{
	return lhs.channel_name_ < rhs.channel_name_;
}
inline bool operator<=(const RxClientWrapper &lhs, const RxClientWrapper &rhs)
{
	return !(rhs < lhs);
}
inline bool operator>(const RxClientWrapper &lhs, const RxClientWrapper &rhs)
{
	return rhs < lhs;
}
inline bool operator>=(const RxClientWrapper &lhs, const RxClientWrapper &rhs)
{
	return !(lhs < rhs);
}
