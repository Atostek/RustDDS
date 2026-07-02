#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/DataReaderListener.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

using namespace eprosima::fastdds::dds;

class ShapeSubscriber
{
public:
    ShapeSubscriber();

    ~ShapeSubscriber();

    bool init(bool with_security);

    void run();

private:
    DomainParticipant *participant_;
    Subscriber *subscriber_;
    Topic *topic_;
    DataReader *reader_;
    TypeSupport type_;

    class SubscriberListener : public DataReaderListener
    {
    public:
        void on_data_available(DataReader *reader);
        void on_subscription_matched(DataReader *reader, const SubscriptionMatchedStatus &info);
        int matched = 0;
        int received_samples = 0;
    } listener_;
};