#pragma once

#include "my_amqp_controller.hpp"

class TxClientWrapper
{
public:
	explicit TxClientWrapper(const std::string &channel_name
		, const std::shared_ptr<rmq::ChannelListener> &listener
		, const rmq::MyTxDataQueuePtr& queue
		) :
	channel_name_(channel_name), listener_(listener), queue_(queue)
	{}

	std::string getChannelName() const { return channel_name_; }
	std::shared_ptr<rmq::ChannelListener> getListener() const  { return listener_; }
	rmq::MyTxDataQueuePtr getQueue() const { return queue_; }

	// Declare the operators as friends but define them outside
	friend bool operator<(const TxClientWrapper& lhs, const TxClientWrapper& rhs);
	friend bool operator<=(const TxClientWrapper& lhs, const TxClientWrapper& rhs);
	friend bool operator>(const TxClientWrapper& lhs, const TxClientWrapper& rhs);
	friend bool operator>=(const TxClientWrapper& lhs, const TxClientWrapper& rhs);

private:
	std::string channel_name_;
	std::shared_ptr<rmq::ChannelListener> listener_;
	rmq::MyTxDataQueuePtr queue_;
};


inline bool operator<(const TxClientWrapper &lhs, const TxClientWrapper &rhs)
{
	return lhs.channel_name_ < rhs.channel_name_;
}
inline bool operator<=(const TxClientWrapper &lhs, const TxClientWrapper &rhs)
{
	return !(rhs < lhs);
}
inline bool operator>(const TxClientWrapper &lhs, const TxClientWrapper &rhs)
{
	return rhs < lhs;
}
inline bool operator>=(const TxClientWrapper &lhs, const TxClientWrapper &rhs)
{
	return !(lhs < rhs);
}
