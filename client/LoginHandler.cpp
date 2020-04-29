#include "StdAfx.h"
#include "LoginHandler.h"
#include "common/RDMUtils.h"
#include "common/AppUtil.h"

using namespace std;

LoginHandler::LoginHandler(rfa::sessionLayer::OMMConsumer* pOMMConsumer,
                           rfa::common::EventQueue& eventQueue,
                           rfa::common::Client& client,
                           rfa::logger::ComponentLogger& componentLogger) :
_pLoginHandle(0),
_pOMMConsumer(pOMMConsumer),
_pOMMProvider(0),
_eventQueue(eventQueue),
_client(client),
_isLoggedIn(false),
_receivedLoginStatus(false),
_componentLogger(componentLogger),
_log(""),
_statusText("")
{
}

LoginHandler::LoginHandler(rfa::sessionLayer::OMMProvider* pOMMProvider,
                           rfa::common::EventQueue& eventQueue,
                           rfa::common::Client& client,
                           rfa::logger::ComponentLogger& componentLogger) :
_pLoginHandle(0),
_pOMMConsumer(0),
_pOMMProvider(pOMMProvider),
_eventQueue(eventQueue),
_client(client),
_isLoggedIn(false),
_receivedLoginStatus(false),
_componentLogger(componentLogger),
_log(""),
_statusText("")
{
}

LoginHandler::~LoginHandler(void)
{
}

void LoginHandler::cleanup() {
    if(_pLoginHandle) {
        if(_pOMMConsumer) {
            _pOMMConsumer->unregisterClient(_pLoginHandle);
        } else if(_pOMMProvider) {
            _pOMMProvider->unregisterClient(_pLoginHandle);
        }
    }
}

void LoginHandler::sendRequest(const std::string &user, const std::string &instance, const std::string &appid, const std::string &pos){
    rfa::message::ReqMsg reqMsg;
    rfa::message::AttribInfo attribInfo;

    reqMsg.setMsgModelType( rfa::rdm::MMT_LOGIN );
    // Login requests must be streaming
    reqMsg.setInteractionType( rfa::message::ReqMsg::InitialImageFlag | rfa::message::ReqMsg::InterestAfterRefreshFlag );
    attribInfo.setNameType( rfa::rdm::USER_NAME );

    // if DACS is in use, this must be a valid DACS username.
    const rfa::common::RFA_String userName( user.c_str() );
    attribInfo.setName( userName );

    // The following is optional for use with DACS */
    //encode the ApplicationId and the Position into the AttribInfo
    rfa::data::ElementList elementList;
    rfa::data::ElementEntry element;
    rfa::data::DataBuffer elementData;

    rfa::data::ElementListWriteIterator elwiter;
    elwiter.start( elementList );

    // set the Application ID (default 256)
    const rfa::common::RFA_String appIdName( "ApplicationId" );
    element.setName( appIdName );
    const rfa::common::RFA_String appId( appid.c_str() );
    elementData.setFromString( appId, rfa::data::DataBuffer::StringAsciiEnum );
    element.setData( elementData );
    elwiter.bind( element );

    // set the Position to <IP-address of this machine>/<process-id>
    const rfa::common::RFA_String positionName("Position");
    element.setName( positionName );
    rfa::common::RFA_String position( pos.c_str() );
    if(pos.empty()) {
        getDefPosition(position);
    }
    elementData.setFromString(position, rfa::data::DataBuffer::StringAsciiEnum );
    element.setData( elementData );
    elwiter.bind( element );

    rfa::common::RFA_String instanceId(instance.c_str());
    if(instanceId.length() != 0) {
        element.setName(rfa::rdm::ENAME_INST_ID);
        elementData.setFromString(instanceId, rfa::data::DataBuffer::StringAsciiEnum );
        element.setData( elementData );
        elwiter.bind( element );
    }

    elwiter.complete();
    attribInfo.setAttrib( elementList );
    // end Optional DACS code

    reqMsg.setAttribInfo( attribInfo );

    rfa::sessionLayer::OMMItemIntSpec OMMItemIntSpec;
    OMMItemIntSpec.setMsg( &reqMsg );

    if(_pOMMConsumer) {
        _pLoginHandle = _pOMMConsumer->registerClient( &_eventQueue, &OMMItemIntSpec, _client , NULL );
    } else if(_pOMMProvider) {
        _pLoginHandle = _pOMMProvider->registerClient( &_eventQueue, &OMMItemIntSpec, _client , NULL );
    }
}

void LoginHandler::closeRequest(){
}

void LoginHandler::pauseAll(const std::string &user) {
    rfa::message::ReqMsg reqMsg;
    rfa::message::AttribInfo attribInfo;
    reqMsg.setMsgModelType( rfa::rdm::MMT_LOGIN );
    reqMsg.setInteractionType( rfa::message::ReqMsg::PauseFlag );
    attribInfo.setNameType( rfa::rdm::USER_NAME );
    const rfa::common::RFA_String userName( user.c_str() );
    attribInfo.setName( userName );
    reqMsg.setAttribInfo( attribInfo );
    rfa::sessionLayer::OMMItemIntSpec OMMItemIntSpec;
    OMMItemIntSpec.setMsg( &reqMsg );
    if(_pOMMConsumer)
        _pOMMConsumer->reissueClient(_pLoginHandle, &OMMItemIntSpec);
}

void LoginHandler::resumeAll(const std::string &user) {
    rfa::message::ReqMsg reqMsg;
    rfa::message::AttribInfo attribInfo;
    reqMsg.setMsgModelType( rfa::rdm::MMT_LOGIN );
    reqMsg.setInteractionType( rfa::message::ReqMsg::InterestAfterRefreshFlag );
    attribInfo.setNameType( rfa::rdm::USER_NAME );
    const rfa::common::RFA_String userName( user.c_str() );
    attribInfo.setName( userName );
    reqMsg.setAttribInfo( attribInfo );
    rfa::sessionLayer::OMMItemIntSpec OMMItemIntSpec;
    OMMItemIntSpec.setMsg( &reqMsg );
    if(_pOMMConsumer)
        _pOMMConsumer->reissueClient(_pLoginHandle, &OMMItemIntSpec);
}

void LoginHandler::processResponse( const rfa::message::RespMsg& respMsg){

    switch(respMsg.getRespType())
    {
    case rfa::message::RespMsg::RefreshEnum:
        {
            // On Refresh - To see if the login has been accepted by the server we need to check
            // if the StreamState is Open
            if(respMsg.getHintMask() &  rfa::message::RespMsg::RespStatusFlag)
            {
                const rfa::common::RespStatus & status = respMsg.getRespStatus();
                _log = "[LoginHandler::processResponse] Login Refresh Msg :";
                _log += " streamState=\"";
                _log.append(RDMUtils::streamStateToString(status.getStreamState()).c_str());
                _log += "\"";
                if(respMsg.getRespStatus().getStreamState() == rfa::common::RespStatus::OpenEnum) {
                    _componentLogger.log(LM_GENERIC_ONE,rfa::common::Information,_log.c_str());
                    // non-interactive provider doesn't need to login (this may change in the future)
                    if(_pOMMProvider)
                        _isLoggedIn  = true;
                } else {
                    _componentLogger.log(LM_GENERIC_ONE,rfa::common::Error,_log.c_str());
                }
            }
            break;
        }
    case rfa::message::RespMsg::StatusEnum:
        {
            // On Status - A forced log off or login rejected is identified msg with a StreamState
            // of Closed and a Status code of NotAuthorized
            if(respMsg.getHintMask() &  rfa::message::RespMsg::RespStatusFlag)
            {
                _receivedLoginStatus = true;
                const rfa::common::RespStatus & status = respMsg.getRespStatus();
                _statusText.append(status.getStatusText().c_str());
                if (( status.getStreamState() == rfa::common::RespStatus::ClosedEnum ) ||
                    ( status.getStreamState() == rfa::common::RespStatus::ClosedRecoverEnum ) ||
                    ( respMsg.getRespStatus().getStatusCode() == rfa::common::RespStatus::NotAuthorizedEnum))
                {
                    _log = "[LoginHandler::processResponse] Login Status :";
                    _log += " dataState=\"";
                    _log.append(RDMUtils::dataStateToString(status.getDataState()).c_str());
                    _log += "\"";
                    _log += " streamState=\"";
                    _log.append(RDMUtils::streamStateToString(status.getStreamState()).c_str());
                    _log += "\"";
                    _log += " statusCode=\"";
                    _log.append(RDMUtils::statusCodeToString(status.getStatusCode()).c_str());
                    _log += "\"";
                    _log += " statusText=\"";
                    _log.append(status.getStatusText().c_str());
                    _log += "\"";
                    _componentLogger.log(LM_GENERIC_ONE,rfa::common::Error,_log.c_str());
                    _isLoggedIn  = false;   //Rejected login or forced log off
                    // Note: LoginManager currently does not retry login for ClosedRecoverEnum
                }
                else
                {
                    _log = "[LoginHandler::processResponse] Login Status :";
                    _log += " dataState=\"";
                    _log.append(RDMUtils::dataStateToString(status.getDataState()).c_str());
                    _log += "\"";
                    _log += " streamState=\"";
                    _log.append(RDMUtils::streamStateToString(status.getStreamState()).c_str());
                    _log += "\"";
                    _log += " statusCode=\"";
                    _log.append(RDMUtils::statusCodeToString(status.getStatusCode()).c_str());
                    _log += "\"";
                    _log += " statusText=\"";
                    _log.append(status.getStatusText().c_str());
                    _log += "\"";
                    _componentLogger.log(LM_GENERIC_ONE,rfa::common::Information,_log.c_str());
                    _isLoggedIn  = true;
                }
            }
            else
            {
                cout<<endl<<"[LoginHandler::processResponse] ERROR: LoginManager::processEvent() received status msg with no RespStatus"<<endl;
                //exit(-1);
            }
            break;
        }
    case rfa::message::RespMsg::UpdateEnum:
        {
            // in the future we could receive an update with a new permission profile
            std::cout << std::endl << "[LoginHandler::processResponse] LoginManager::processEvent() received update msg. Currently unsupported" << std::endl;
            break;
        }
    }
}

bool LoginHandler::isLoggedIn() const {
    return _isLoggedIn;
}

bool LoginHandler::receivedLoginStatus() const {
    return _receivedLoginStatus;
}

rfa::common::RFA_String LoginHandler::getStatusText() const {
    return _statusText;
}
