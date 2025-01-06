
// Programming Challenge from Akuna Capital to Randy Taylor
// drandalltaylor@gmail.com
// LinkedIn:  https://www.linkedin.com/in/drandalltaylor

// Note: the test input data has 1 error: there is an Ack from the exchange, but the direction is TO_EXCH.  See line 405

#include <stdio.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>
#include <map>
#include <memory>
#include <set>
#include <string.h>

using namespace std;

enum class MsgType : uint8_t
{
        ENTRY = 1,
        ACK = 2,
        FILL = 3
};

enum class MsgDirection : uint8_t
{
        TO_EXCH = 0,
        FROM_EXCH = 1
};

enum class TradeSide : uint8_t
{
        BUY = 1,
        SELL = 2
};

enum class OrderStatus : uint8_t
{
        GOOD = 1,
        REJECT = 2
};

enum class RejectCode : uint8_t
{
        NONE = 0,
        INVALID_PRODUCT = 1,
        INVALID_PRICE = 2,
        INVALID_QTY = 3
};

enum class TimeInForce : uint8_t
{
        TIF_IOC = 1,
        TIF_GFD = 2
};

const char msg_termination_string[9] = "DBDBDBDB";

#pragma pack(push,1)

struct Header
{
        uint16_t marker; // "ST"
        MsgType msg_type; // 1=OrderEntry 2=OrderAck 3=OrderFill
        uint64_t sequence_id;
        uint64_t timestamp;
        MsgDirection msg_direction;
        uint16_t msg_len;
};

typedef uint64_t client_id_type;
typedef uint32_t order_id_type;

struct OrderEntry : public Header
{
        uint64_t price; // divide by 10000 for decimal price
        uint32_t qty;
        char instrument[10];
        TradeSide side; // 1=buy 2=sell
        client_id_type client_id;
        TimeInForce time_in_force; // 1=IOC 2=GFD
        char trader_tag[3];
        uint8_t firm_id;
        char firm[256];
        char termination_string[8]; // DBDBDBDB
};

std::ostream& operator<<(std::ostream& o, const OrderEntry& m)
{
        o << "OrderEntry" << " " << m.client_id << " ";
        //o << std::string(&m.trader_tag[0], sizeof(m.trader_tag));
        return o;
}

struct OrderAck : public Header
{
        order_id_type order_id;
        client_id_type client_id;
        OrderStatus order_status; // 1=good 2=reject
        RejectCode reject_code; // 0=no code 1=invalid product 2=invalid price 3 invalid qty
        char termination_string[8]; // DBDBDBDB
};

struct CounterParty
{
        uint8_t firm_id;
        char trader_tag[3];
        uint32_t qty;
};

struct OrderFill : public Header
{
        order_id_type order_id;
        uint64_t fill_price; // divide by 10000 to get decimal price
        uint32_t fill_qty;
        uint8_t no_of_contras; // number of counter-parties on the fill
        //CounterParty contras[N]; <-- keep as documentation
        //char termination_string[8]; <-- keep as documentation. DBDBDBDB
};

#pragma pack(pop)

class Msg
{
public:
        explicit Msg(char* message, size_t len, MsgType msg_type, CounterParty* counter_parties = nullptr, uint8_t num_contras = 0)
                : len(len), msg_type(msg_type), counter_parties(counter_parties), num_contras(num_contras)
        {
                msg.reset(new char[len]);
                ::memcpy(msg.get(), message, len);
        }
        Header* GetHeader() { return reinterpret_cast<Header*>(msg.get()); }
        OrderEntry* GetAsOrderEntry() { return reinterpret_cast<OrderEntry*>(msg.get()); }
        OrderFill* GetAsOrderFill() { return reinterpret_cast<OrderFill*>(msg.get()); }
        OrderAck* GetAsOrderAck() { return reinterpret_cast<OrderAck*>(msg.get()); }
        CounterParty* GetCounterParty() { return counter_parties; }
        uint8_t GetCountCounterParty() { return num_contras; }
        MsgType GetMsgType() { return MsgType(GetHeader()->msg_type); }
private:
        std::unique_ptr<char> msg; // entire msg, including header
        size_t len;
        MsgType msg_type;
        CounterParty* counter_parties; // points in the msg
        uint8_t num_contras;
};

typedef std::shared_ptr<Msg> MsgPtrType;

class EndOfDataException : public std::exception
{
public:
  EndOfDataException(const char* what)
    : std::exception(/*what*/)
  {}
};

class MalformedInputException : public std::exception
{
public:
        MalformedInputException(const char* what)
          : std::exception(/*what*/)
        {}
};

class Producer
{
public:
        explicit Producer(std::istream &input)
                : input(input)
        {}
        MsgPtrType GetMsg()
        {
                vector<char> buf;
                buf.resize(sizeof(Header));
                while (!input.eof()) {
                        input.read(&buf[0], sizeof(Header));
                        if (input.eof() && input.fail()) {
                                throw EndOfDataException("eof");
                        }
                        Header* header = reinterpret_cast<Header*>(&buf[0]);
                        auto remainder = header->msg_len;
                        buf.resize(buf.size() + remainder);
                        header = reinterpret_cast<Header*>(&buf[0]);
                        char*pc = &buf[0] + sizeof(Header);
                        input.read(pc, remainder);
                        if (input.eof() && input.fail()) {
                                throw EndOfDataException("eof");
                        }
                        switch (header->msg_type) {
                        case MsgType::FILL: {
                                OrderFill* fill = reinterpret_cast<OrderFill*>(&buf[0]);
                                CounterParty* contras = fill->no_of_contras == 0 ? nullptr :
                                        reinterpret_cast<CounterParty*>(&pc[0] + sizeof(OrderFill));
                                return MsgPtrType(new Msg(&buf[0], buf.size(), fill->msg_type,
                                        contras, fill->no_of_contras));
                        }
                        case MsgType::ACK: {
                                OrderAck* ack = reinterpret_cast<OrderAck*>(&buf[0]);
                                return MsgPtrType(new Msg(&buf[0], buf.size(), ack->msg_type));
                                break;
                        }
                        case MsgType::ENTRY: {
                                OrderEntry* order = reinterpret_cast<OrderEntry*>(&buf[0]);
                                return MsgPtrType(new Msg(&buf[0], buf.size(), order->msg_type));
                                break;
                        }
                        default:
                                throw MalformedInputException("bad input");
                        }
                }
        }

private:
        std::istream& input;
};

typedef std::string trader_tag_type;

class OrderBook
{
public:
        void Add(MsgPtrType& order_msg) {
                OrderEntry* order = order_msg->GetAsOrderEntry();
                auto pr = pending_orders.insert(make_pair(order->client_id, order_msg));
                assert(pr.second);

                // build client <--> trader maps
                auto itr = client_to_traders.find(order->client_id);
                if (itr == client_to_traders.end()) {
                        set<string> s;
                        s.insert(string(order->trader_tag, sizeof(order->trader_tag)));
                        client_to_traders.insert(make_pair(order->client_id, s));
                } else {
                        itr->second.insert(string(order->trader_tag, sizeof(order->trader_tag)));
                }
                auto itr2 = trader_to_clients.find(string(order->trader_tag, sizeof(order->trader_tag)));
                if (itr2 == trader_to_clients.end()) {
                        set<client_id_type> c;
                        c.insert(order->client_id);
                        auto pr = make_pair(string(order->trader_tag, sizeof(order->trader_tag)), c);
                        trader_to_clients.insert(pr);
                } else {
                        itr2->second.insert(order->client_id);
                }
        }
        void Ack(MsgPtrType& ack_msg) {
                OrderAck* ack = ack_msg->GetAsOrderAck();
                auto itr = pending_orders.find(ack->client_id);
                if (itr == pending_orders.end()) {
                        assert(false);
                        return;
                }
                if (ack->order_status == OrderStatus::GOOD) {
                        auto pr = resting_orders.insert(make_pair(ack->order_id, itr->second));
                        assert(pr.second);
                        MsgPtrType m = itr->second;
                        auto order = m->GetAsOrderEntry();
                        pending_orders.erase(itr);
                        if (order->time_in_force == TimeInForce::TIF_GFD) {
                                trader_tag_type trader(order->trader_tag, sizeof(order->trader_tag));
                                auto itr = liquidity_per_trader.find(trader);
                                if (itr != liquidity_per_trader.end()) {
                                        itr->second += order->qty;
                                }
                                else {
                                        liquidity_per_trader.insert(make_pair(trader, order->qty));
                                }
                        }
                }
                else if (ack->order_status == OrderStatus::REJECT) {
                        pending_orders.erase(itr);
                }
                else {
                        // TODO: log msg
                }
        }
        void Fill(MsgPtrType& fill_msg) {
                OrderFill* fill = fill_msg->GetAsOrderFill();
                auto itr = resting_orders.find(fill->order_id);
                if (itr != resting_orders.end()) {
                        MsgPtrType m = itr->second;
                        auto order = m->GetAsOrderEntry();
                        CounterParty* contra = fill_msg->GetCounterParty();
                        auto contra_count = fill_msg->GetCountCounterParty();
                        assert(order->qty >= fill->fill_qty);
                        order->qty -= fill->fill_qty;
                        if (order->qty == 0) {
                                resting_orders.erase(itr);
                        }
                        auto inst_itr = vol_per_instrument.find(string(order->instrument, sizeof(order->instrument)));
                        if (inst_itr != vol_per_instrument.end()) {
                                inst_itr->second += fill->fill_qty;
                        }
                        else {
                                vol_per_instrument.insert(make_pair(string(order->instrument, sizeof(order->instrument)), fill->fill_qty));
                        }

                        auto trader_itr = vol_per_trader.find(string(order->trader_tag, sizeof(order->trader_tag)));
                        if (trader_itr != vol_per_trader.end()) {
                                trader_itr->second += fill->fill_qty;
                                for (auto i = 0; i < contra_count; ++i) {
                                        trader_itr->second += (contra + i)->qty;
                                }
                        }
                        else {
                                auto vol = fill->fill_qty;
                                for (auto i = 0; i < contra_count; ++i) {
                                        vol += (contra + i)->qty;
                                }
                                vol_per_trader.insert(make_pair(string(order->trader_tag, sizeof(order->trader_tag)), vol));
                        }
                } else {
                        assert(false);
                }
        }
        std::string GetMostActiveTrader() {
                // highest filled volume, counting contra's qty & original order qty filled
                string trader;
                uint64_t vol(0);
                for (auto itr = vol_per_trader.begin(); itr != vol_per_trader.end(); ++itr) {
                        if (itr->second > vol) {
                                vol = itr->second;
                                trader = itr->first;
                        }
                }
                assert(!trader.empty());
                return trader;
        }
        std::string GetMostLiquidTrader() {
                // highest qty of GFD orders entered to market
                string trader;
                uint64_t vol(0);
                for (auto itr = liquidity_per_trader.begin(); itr != liquidity_per_trader.end(); ++itr) {
                        if (itr->second > vol) {
                                vol = itr->second;
                                trader = itr->first;
                        }
                }
                assert(!trader.empty());
                return trader;
        }
        void OutputVolumePerInstrument(std::ostream& os) {
                for (auto itr = vol_per_instrument.begin(); itr != vol_per_instrument.end(); ++itr) {
                        os << ", ";
                        os << itr->first.c_str();
                        os << ":" << itr->second;
                }
        }
        void OutputClientTraderRatios(std::ostream& os) {
                for (auto itr = client_to_traders.begin(); itr != client_to_traders.end(); ++itr) {
                        if (itr->second.size() > 1) {
                                //
                        }
                }
                for (auto itr = trader_to_clients.begin(); itr != trader_to_clients.end(); ++itr) {
                        if (itr->second.size() > 1) {
                                //
                        }
                }
        }
private:

        map<client_id_type, MsgPtrType> pending_orders;
        map<order_id_type, MsgPtrType> resting_orders;

        map<string, uint64_t> vol_per_instrument;
        map<trader_tag_type, uint64_t> liquidity_per_trader; // to find trader with highest GFD volume on OrderEntry
        map<string, uint64_t> vol_per_trader; // to find trader with highest filled volume (counting all Contra qty and the trader's qty)

        std::map<client_id_type, std::set<std::string> > client_to_traders;
        std::map<std::string, std::set<uint64_t> > trader_to_clients;
};

int main(int argc, char**argv)
{
        assert(sizeof(RejectCode) == 1);
        int rc(0);
        if (argc < 2) {
                return -1;
        }
        ifstream input(argv[1], ios::binary | ios::in);
        if (!input.is_open()) {
                return -1;
        }
        Producer producer(input);
        OrderBook book;

        uint32_t total_packets(0);
        uint32_t order_entry_msg_count(0);
        uint32_t order_ack_msg_count(0);
        uint32_t order_fill_msg_count(0);

        try {
                while (true) {
                        try {
                                MsgPtrType msg = producer.GetMsg();
                                Header* header = reinterpret_cast<Header*>(msg->GetHeader());
                                ++total_packets;
                                switch (msg->GetMsgType()) {
                                case MsgType::ACK: {
                                        OrderAck* ack = reinterpret_cast<OrderAck*>(header);
                                        //assert(ack->msg_direction == MsgDirection::FROM_EXCH);  <-- input data is bad. Ack is only from Exch
                                        ++order_ack_msg_count;
                                        book.Ack(msg);
                                        break;
                                }
                                case MsgType::ENTRY: {
                                        OrderEntry* order = reinterpret_cast<OrderEntry*>(header);
                                        assert(order->msg_direction == MsgDirection::TO_EXCH);
                                        ++order_entry_msg_count;
                                        book.Add(msg);
                                        break;
                                }
                                case MsgType::FILL: {
                                        OrderFill* fill = reinterpret_cast<OrderFill*>(header);
                                        assert(fill->msg_direction == MsgDirection::FROM_EXCH);
                                        ++order_fill_msg_count;
                                        book.Fill(msg);
                                        break;
                                }
                                }
                        }
                        catch (MalformedInputException& ex) {
                                assert(false);
                                continue;
                        }
                }
        }
        catch (EndOfDataException& ex) {
        }

        //book.OutputClientTraderRatios(std::cout);

        printf("%u, %u, %u, %u, %s, %s", total_packets, order_entry_msg_count, order_ack_msg_count, order_fill_msg_count
                , book.GetMostActiveTrader().c_str(), book.GetMostLiquidTrader().c_str());
        book.OutputVolumePerInstrument(std::cout);
        return rc;
}

