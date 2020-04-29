#ifndef LOGINHANDLER_H
#define LOGINHANDLER_H

class LoginHandler
{
public:
    LoginHandler(rfa::sessionLayer::OMMConsumer* pOMMConsumer,
                    rfa::common::EventQueue& eventQueue,
                    rfa::common::Client& client,
                    rfa::logger::ComponentLogger& componentLogger);

    LoginHandler(rfa::sessionLayer::OMMProvider* pOMMProvider,
                    rfa::common::EventQueue& eventQueue,
                    rfa::common::Client& client,
                    rfa::logger::ComponentLogger& componentLogger);

    ~LoginHandler(void);

    void sendRequest(const std::string &user, const std::string &instance, const std::string &appid, const std::string &pos);
    void closeRequest();
    void pauseAll(const std::string &user);
    void resumeAll(const std::string &user);
    void processResponse(const rfa::message::RespMsg& respMsg);
    bool isLoggedIn() const;
    bool receivedLoginStatus() const;
    void cleanup();
    rfa::common::RFA_String getStatusText() const;

    rfa::common::Handle             *_pLoginHandle;

private:
    rfa::sessionLayer::OMMConsumer  *_pOMMConsumer;
    rfa::sessionLayer::OMMProvider  *_pOMMProvider;
    rfa::common::EventQueue         &_eventQueue;
    rfa::common::Client             &_client;
    bool                            _isLoggedIn;
    bool                            _receivedLoginStatus;
    rfa::logger::ComponentLogger    &_componentLogger;
    rfa::common::RFA_String         _log;
    rfa::common::RFA_String         _statusText;
};

#endif
