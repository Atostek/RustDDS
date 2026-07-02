#include "ShapeSubscriber.hpp"
#include "ShapePubSubTypes.h"

#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/qos/SubscriberQos.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>

#include <unistd.h>
#include <signal.h>
#include <thread>

using namespace eprosima::fastdds::dds;

ShapeSubscriber::ShapeSubscriber() : participant_(nullptr), subscriber_(nullptr), topic_(nullptr), reader_(nullptr), type_(new ShapeTypePubSubType()), listener_() {}

ShapeSubscriber::~ShapeSubscriber()
{
    if (reader_ && subscriber_)
    {
        subscriber_->delete_datareader(reader_);
    }

    if (subscriber_ && participant_)
    {
        participant_->delete_subscriber(subscriber_);
    }

    if (topic_ && participant_)
    {
        participant_->delete_topic(topic_);
    }
    if (participant_)
    {
        DomainParticipantFactory::get_instance()->delete_participant(participant_);
    }
}

bool ShapeSubscriber::init(bool with_security)
{
    DomainParticipantQos participant_qos;
    participant_qos.name("subscriber_participant");

    if (with_security)
    {
        using namespace std;
        string example_security_configuration_path = "file://../../examples/security_configuration_files/";
        string dds_sec = "dds.sec.";
        string auth = dds_sec + "auth.";
        string auth_plugin = "builtin.PKI-DH";
        string auth_prefix = auth + auth_plugin + ".";
        string access = dds_sec + "access.";
        string access_plugin = "builtin.Access-Permissions";
        string access_prefix = access + access_plugin + ".";
        string crypto = dds_sec + "crypto.";
        string crypto_plugin = "builtin.AES-GCM-GMAC";
        string plugin = "plugin";

        std::vector<pair<string, string>> security_properties = {
            pair<string, string>(auth + plugin, auth_plugin),
            pair<string, string>(access + plugin, access_plugin),
            pair<string, string>(crypto + plugin, crypto_plugin),
            pair<string, string>(auth_prefix + "identity_ca", example_security_configuration_path + "identity_ca.cert.pem"),
            pair<string, string>(auth_prefix + "identity_certificate", example_security_configuration_path + "cert.pem"),
            pair<string, string>(auth_prefix + "private_key", example_security_configuration_path + "key.pem"),
            pair<string, string>(access_prefix + "permissions_ca", example_security_configuration_path + "permissions_ca.cert.pem"),
            pair<string, string>(access_prefix + "governance", example_security_configuration_path + "governance.p7s"),
            pair<string, string>(access_prefix + "permissions", example_security_configuration_path + "permissions.p7s"),
        };

        for (pair<string, string> property : security_properties)
        {
            participant_qos.properties().properties().emplace_back(property.first, property.second);
        }
    }

    participant_ = DomainParticipantFactory::get_instance()->create_participant(0, participant_qos);

    if (participant_)
    {
        type_.register_type(participant_);
    }

    SubscriberQos subscriber_qos = SUBSCRIBER_QOS_DEFAULT;

    if (participant_)
    {
        subscriber_ = participant_->create_subscriber(subscriber_qos);
    }

    TopicQos topic_qos = TOPIC_QOS_DEFAULT;

    if (participant_)
    {
        topic_ = participant_->create_topic("Square", type_.get_type_name(), topic_qos);
    }

    DataReaderQos datareader_qos = DATAREADER_QOS_DEFAULT;

    if (subscriber_ && topic_)
    {
        reader_ = subscriber_->create_datareader(topic_, datareader_qos, &listener_);
    }

    if (reader_ && topic_ && subscriber_ && participant_)
    {
        std::cout << "DataReader created for the topic Square." << std::endl;
        return true;
    }
    else
    {
        return false;
    }
}

// For handling stop signal to break the infinite loop
namespace subscriber_stop
{
    volatile sig_atomic_t stop;
    void handle_interrupt(int)
    {
        stop = 1;
    }
}

void ShapeSubscriber::run()
{
    signal(SIGINT, subscriber_stop::handle_interrupt);

    std::cout << "Waiting for data" << std::endl;

    while (!subscriber_stop::stop)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    std::cout << "\nStopped" << std::endl;
}

void ShapeSubscriber::SubscriberListener::on_data_available(DataReader *reader)
{
    ShapeType sample;
    SampleInfo info;

    ReturnCode_t return_code = reader->take_next_sample(&sample, &info);
    if (return_code == ReturnCode_t::RETCODE_OK)
    {
        ++received_samples;
        std::cout << "Sample received [" << sample.color() << ": (" << sample.x() << "," << sample.y() << ")], count=" << received_samples << std::endl;
    }
    else
    {
        std::cout << "Read failed: return code " << return_code() << std::endl;
    }
}

void ShapeSubscriber::SubscriberListener::on_subscription_matched(DataReader *, const SubscriptionMatchedStatus &info)
{
    matched = info.current_count;
    std::cout << "Number of matched writers matched: " << matched << std::endl;
}