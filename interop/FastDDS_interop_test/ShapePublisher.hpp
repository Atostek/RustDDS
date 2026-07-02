#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>

using namespace eprosima::fastdds::dds;

class ShapePublisher
{
public:
    ShapePublisher();

    ~ShapePublisher();

    bool init(bool with_security);

    void run();

private:
    DomainParticipant *participant_;
    Publisher *publisher_;
    Topic *topic_;
    DataWriter *writer_;
    TypeSupport type_;

    class SubscriberListener : public DataWriterListener
    {
    public:
        void on_publication_matched(DataWriter *writer, const PublicationMatchedStatus &info);
        int matched = 0;
    } listener_;
};