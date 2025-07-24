#pragma once

namespace rmq
{
class RxClientWrapper
{
public:
	explicit RxClientWrapper(const std::string &channel_name
		, const ChannelListenerPtr &listener
		, const MyRxDataQueuePtr& data_queue
		, const MyRxAckQueuePtr& ack_queue) :
	channel_name_(channel_name), listener_(listener), data_queue_(data_queue), ack_queue_(ack_queue)
	{}

	std::string getChannelName() const { return channel_name_; }
	ChannelListenerPtr getListener() const  { return listener_; }
	MyRxDataQueuePtr getQueue() const { return data_queue_; }
	MyRxAckQueuePtr getAckQueue() const { return ack_queue_; }
	void acknowledge(const rmq::IMessageAck& ack) const { ack_queue_->push(ack); }

	// Declare the operators as friends but define them outside
	friend bool operator<(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator<=(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator>(const RxClientWrapper& lhs, const RxClientWrapper& rhs);
	friend bool operator>=(const RxClientWrapper& lhs, const RxClientWrapper& rhs);

private:
	std::string channel_name_;
	ChannelListenerPtr listener_;
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

}
