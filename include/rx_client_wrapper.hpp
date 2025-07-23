#pragma once

#include "my_amqp_controller.hpp"

class RxClientWrapper
{
public:
	explicit RxClientWrapper(const std::string &channel_name
		, const std::shared_ptr<rmq::ChannelListener> &listener
		, const rmq::MyRxDataQueuePtr& data_queue
		, const rmq::MyRxAckQueuePtr& ack_queue) :
	channel_name_(channel_name), listener_(listener), data_queue_(data_queue), ack_queue_(ack_queue)
	{}

	std::string getChannelName() const { return channel_name_; }
	std::shared_ptr<rmq::ChannelListener> getListener() const  { return listener_; }
	rmq::MyRxDataQueuePtr getQueue() const { return data_queue_; }
	rmq::MyRxAckQueuePtr getAckQueue() const { return ack_queue_; }
	void acknowledge(const rmq::IMessageAck& ack) const { ack_queue_->push(ack); }

	// Declare the operators as friends but define them outside
	friend bool operator<(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator<=(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator>(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator>=(const RxClientWrapper& lhs, const RxClientWrapper& rhs);

private:
	std::string channel_name_;
	std::shared_ptr<rmq::ChannelListener> listener_;
	rmq::MyRxDataQueuePtr data_queue_;
	rmq::MyRxAckQueuePtr ack_queue_;
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
