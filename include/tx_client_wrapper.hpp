#pragma once


namespace rmq
{
class TxClientWrapper
{
public:
	explicit TxClientWrapper(const std::string &channel_name
		, const ChannelListenerPtr &listener
		, const MyTxDataQueuePtr& queue
		) :
	channel_name_(channel_name), listener_(listener), queue_(queue)
	{}

	std::string getChannelName() const { return channel_name_; }
	ChannelListenerPtr getListener() const  { return listener_; }
	MyTxDataQueuePtr getQueue() const { return queue_; }

	// Declare the operators as friends but define them outside
	friend bool operator<(const TxClientWrapper& lhs, const TxClientWrapper& rhs);
	friend bool operator<=(const TxClientWrapper& lhs, const TxClientWrapper& rhs);
	friend bool operator>(const TxClientWrapper& lhs, const TxClientWrapper& rhs);
	friend bool operator>=(const TxClientWrapper& lhs, const TxClientWrapper& rhs);

private:
	std::string channel_name_;
	ChannelListenerPtr listener_;
	MyTxDataQueuePtr queue_;
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

}