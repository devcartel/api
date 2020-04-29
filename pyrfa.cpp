/*
* (C) Copyright 2013 DevCartel. All rights reserved,
* Bangkok, Thailand
*/
#include "pyrfa.h"
#include "client/LoginHandler.h"
#include "client/DirectoryHandler.h"
#include "client/DictionaryHandler.h"
#include "client/SymbolListHandler.h"
#include "client/MarketPriceHandler.h"
#include "client/MarketByOrderHandler.h"
#include "client/MarketByPriceHandler.h"
#include "client/TimeSeriesHandler.h"
#include "client/RDMCProvServer.h"
#include "client/HistoryHandler.h"
#include "client/OMMPost.h"
#include "client/OMMInteractiveProvider.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/python/overloads.hpp>

using namespace std;

extern "C" void checkException(rfa::common::Exception& e);

Pyrfa::Pyrfa() :
    _pConfigDb(0),
    _pEventQueue(0),
    _pLoggerEventQueue(0),
    _pLogger(0),
    _pComponentLogger(0),
    _pLoggerClient(0),
    _pSession(0),
    _pOMMConsumer(0),
    _pOMMProvider(0),
    _pLoginHandler(0),
    _pDirectoryHandler(0),
    _pDictionaryHandler(0),
    _pSymbolListHandler(0),
    _pMarketPriceHandler(0),
    _pMarketByOrderHandler(0),
    _pMarketByPriceHandler(0),
    _pTimeSeriesHandler(0),
    _pTimeSeries(0),
    _pTS1DictDb(0),
    _pHistoryHandler(0),
    _pConnHandle(0),
    _pErrHandle(0),
    _pListConnHandle(0),
    _pClientSessListHandle(0),
    _pLoginToken(0),
    _debug(false),
    _isSymbolListAvailable(false),
    _isHistoryAvailable(false),
    _isPublisher(false),
    _out(),
    _eventData(),
    _log(""),
    _isConnectionUp(false),
    _pOMMCProvServer(0),
    _pOMMInteractiveProvider(0),
    _pOMMPost(0),
    _symbolList(""),
    _watchList(""),
    _timeSeries(""),
    _timeSeriesPeriod(rfa::ts1::TS1Series::Daily),
    _timeSeriesMaxRecords(10),
    _userName(""),
    _interactionType(rfa::message::ReqMsg::InitialImageFlag | rfa::message::ReqMsg::InterestAfterRefreshFlag),
    _viewFIDs(""),
    _fieldDictionaryFilename(""),
    _enumTypeFilename(""),
    _directory("")
{
    initPythonLib();
    initializeRFA();
}

Pyrfa::~Pyrfa()
{
    cleanUp();
}

int Pyrfa::initPythonLib() {
    try {
        //initialize python intepreter.
        Py_Initialize();
        object main_module = import("__main__");
        object main_namespace = main_module.attr("__dict__");
        object ignored = exec("import time", main_namespace);
        return 1;
    } catch( error_already_set ) {
        PyErr_Print();
        return 0;
    }
}

int Pyrfa::initializeRFA() {
    if (!rfa::common::Context::initialize()) {
        cout << "Can not initialize RFA Context" << endl;
        return 1;
    }
    return 0;
}

void Pyrfa::uninitializeRFA() {
    if (rfa::common::Context::getInitializedCount() > 0) {
        rfa::common::Context::uninitialize();
    }
}

void Pyrfa::createConfigDb(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* fileName = extract<const char*>(argv);
    #else
    const char* fileName = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    _pConfigDb = new ConfigDb(fileName);

    // Config loaded. Now read the debug level param
    // Set debug level in Pyrfa namespace
    setDebugMode(object());

    // Set debug level in ConfigDb namespace
    _pConfigDb->setDebugMode(_debug);
    if (_debug)
        cout << "[Pyrfa::createConfigDb] Loading RFA config from file: " << fileName << endl;
}

/*
* print configuration database to stdout. Arguments are the fullnames     of treeNodes
* e.g. Default\\Connections. It also takes variable number of arguments.
* if no argument supplied, it will print all config under "Default" namespace.
*/
void Pyrfa::printConfigDb(object const &argv=object()) {
    const char* fullname = "Default";
    if(argv != object()) {
        #if PY_MAJOR_VERSION >= 3
        fullname = extract<const char*>(argv);
        #else
        fullname = extract<const char*>(str(argv).encode("utf-8"));
        #endif
    }
    if(_debug)
        cout << "[Pyrfa::printConfigDb] Print config db: " << fullname << endl;
    _pConfigDb->printConfigDb( fullname );
}

std::string Pyrfa::getConfigDb(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* configKey = extract<const char*>(argv);
    #else
    const char* configKey = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    return _pConfigDb->getConfigDb(configKey).c_str();
}

/*
* create event queue which will be shared to receive
* all events across this application.
* default name is "EVENTQ1" unless specified
*/
void Pyrfa::createEventQueue() {
    const char* eventQueueName="EVENTQ1";
    const char* loggerEventQueueName="LOGGEREVENTQ";
    if(_debug) {
        cout << "[Pyrfa::createEventQueue] Create an event queue: " << eventQueueName << endl;
        cout << "[Pyrfa::createEventQueue] Create an event queue: " << loggerEventQueueName << endl;
    }
    rfa::common::EventQueue *pEventQueue = rfa::common::EventQueue::create(eventQueueName);
    assert(pEventQueue);
    _pEventQueue = pEventQueue;

    rfa::common::EventQueue *pLoggerEventQueue = rfa::common::EventQueue::create(loggerEventQueueName);
    assert(pLoggerEventQueue);
    _pLoggerEventQueue = pLoggerEventQueue;
}

/*
* create application logger, its name must be the same as RFA context name (RFA)
* the logger will listen to events
*/
void Pyrfa::acquireLogger() {
    const rfa::common::RFA_String &contextName = rfa::common::Context::getName();
    rfa::logger::ApplicationLogger *pLogger = NULL;
    rfa::logger::ComponentLogger *pComponentLogger = NULL;
    try {
        if(_debug)
            cout << "[Pyrfa::acquireLogger] Acquiring a logger on context: " << contextName.c_str() << endl;

        pLogger = rfa::logger::ApplicationLogger::acquire(contextName);

        // Implementation of LogMsgMap for using log internal strings (no *.mc files)
        LogMsgMapImpl *pLogMsgMap = new LogMsgMapImpl();
        pComponentLogger = pLogger->createComponentLogger("Pyrfa", pLogMsgMap);

    } catch (rfa::common::Exception& e) {
        checkException(e);
    }
    if (!pLogger) {
        _log = "[Pyrfa::acquireLogger] Unable to acquire application logger. Terminating ... ";
        pComponentLogger->log(LM_GENERIC_ONE,rfa::common::Error, _log.c_str());
        exit(-2);
    }
    if(_debug) {
        _log = "[Pyrfa::acquireLogger] Got a logger named: " + contextName;
        pComponentLogger->log(LM_GENERIC_ONE,rfa::common::Information, _log.c_str());
    }
    _pLogger = pLogger;
    _pComponentLogger = pComponentLogger;
    _pLoggerClient = new AppLoggerClient(_pLogger, _pLoggerEventQueue);
    _pLoggerClient->setDebugMode(_debug);
}

/*
* rfa session layer
*/
void Pyrfa::acquireSession(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* sessionName = extract<const char*>(argv);
    #else
    const char* sessionName = extract<const char*>(str(argv).encode("utf-8"));
    #endif

    // automatically create event queue and logger
    if(!_pEventQueue)
        createEventQueue();

    if(!_pLogger)
        acquireLogger();

    try {
        rfa::sessionLayer::Session * pSession = rfa::sessionLayer::Session::acquire(sessionName);
        _ASSERTE (pSession);
        _pSession = pSession;
        if(_debug) {
            cout << "[Pyrfa::acquireSession] Session acquired" << endl;
        }
    } catch (rfa::common::Exception& e) {
        checkException(e);
    }
}

void Pyrfa::createOMMConsumer() {
    try {
        //create OMMConsumer
        _pOMMConsumer = _pSession->createOMMConsumer("OMMConsumer");
        assert(_pOMMConsumer);

        // register for ConnectionEvents
        rfa::sessionLayer::OMMConnectionIntSpec connIntSpec;
        _pConnHandle = _pOMMConsumer->registerClient(_pEventQueue, &connIntSpec, *this);
        assert(_pConnHandle);

        rfa::sessionLayer::OMMErrorIntSpec intErrSpec;
        _pErrHandle = _pOMMConsumer->registerClient(_pEventQueue, &intErrSpec, *this);

        if(_debug) {
            _log ="[Pyrfa::createOMMConsumer] Create an OMM consumer client";
            _logInfo(_log.c_str());
        }
    } catch (rfa::common::Exception& e) {
        checkException(e);
    }
}

void Pyrfa::createOMMProvider() {
    try {
        // Lookup for ServiceName, VendorName in configDb for current session
        rfa::common::RFA_String fullname;
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\ServiceName";
        _serviceName = _pConfigDb->getConfigDb( fullname ).c_str();

        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\VendorName";
        _vendorName = _pConfigDb->getConfigDb( fullname ).c_str();

        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\connectionType";
        _connectionType = _pConfigDb->getConfigDb( fullname ).c_str();

        //create OMMProvider
        _pOMMProvider = _pSession->createOMMProvider("OMMProvider", true);

        rfa::sessionLayer::OMMConnectionIntSpec connIntSpec;
        _pConnHandle = _pOMMProvider->registerClient(_pEventQueue, &connIntSpec, *this);
        assert(_pConnHandle);

        if (_connectionType == "RSSL_PROV") {
            //Register for Listener Connection events (these are event related to the server listening port)
            rfa::sessionLayer::OMMListenerConnectionIntSpec listConnIntSpec;
            _pListConnHandle = _pOMMProvider->registerClient(_pEventQueue, &listConnIntSpec, *this);
            assert(_pListConnHandle);

            //Register to receive new client session connection events
            rfa::sessionLayer::OMMClientSessionListenerIntSpec clientSessListIntSpec;
            _pClientSessListHandle = _pOMMProvider->registerClient(_pEventQueue, &clientSessListIntSpec, *this);
            assert(_pClientSessListHandle);
        }

        // Register for Cmd Error events (These events are sent back if the submit() call fails)
        rfa::sessionLayer::OMMErrorIntSpec intErrSpec;
        _pErrHandle = _pOMMProvider->registerClient(_pEventQueue, &intErrSpec, *this);

        if(_debug) {
            _log ="[Pyrfa::createOMMProvider] Create an OMM provider client";
            _logInfo(_log.c_str());
        }
    } catch (rfa::common::Exception& e) {
        checkException(e);
    }
}

void Pyrfa::login(object const &argv1=object(), object const &argv2=object(), object const &argv3=object(), object const &argv4=object()) {
    if (_connectionType == "RSSL_PROV") {
        _logError("[Pyrfa::login] OMM interactive provider login request is not applicable. Skipped.");
        return;
    }

    if (_pLoginHandler)
        return;

    if( argv1 != object()) {
        #if PY_MAJOR_VERSION >= 3
        _userName = extract<string>(argv1);
        #else
        _userName = extract<string>(str(argv1).encode("utf-8"));
        #endif
    }
    std::string instance = "";
    if (argv2 != object()) {
        #if PY_MAJOR_VERSION >= 3
        instance = extract<string>(argv2);
        #else
        instance = extract<string>(str(argv2).encode("utf-8"));
        #endif
    }
    std::string appid = "";
    if (argv3 != object()) {
        #if PY_MAJOR_VERSION >= 3
        appid = extract<string>(argv3);
        #else
        appid = extract<string>(str(argv3).encode("utf-8"));
        #endif
    }
    std::string pos = "";
    if (argv4 != object()) {
        #if PY_MAJOR_VERSION >= 3
        pos = extract<string>(argv4);
        #else
        pos = extract<string>(str(argv4).encode("utf-8"));
        #endif
    }

    // Lookup for UserName, InstanceId, ApplicationId in configDb for current session
    rfa::common::RFA_String fullname;
    if(_userName.empty()) {
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\UserName";
        _userName = _pConfigDb->getConfigDb( fullname ).c_str();
    }
    if (instance.empty()) {
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\InstanceId";
        instance = _pConfigDb->getConfigDb( fullname ).c_str();
    }
    if (appid.empty()) {
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\ApplicationId";
        appid = _pConfigDb->getConfigDb( fullname ).c_str();
        if (appid.empty())
            appid = "256";
    }
    if (pos.empty()) {
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\Position";
        pos = _pConfigDb->getConfigDb( fullname ).c_str();
    }

    if(_pOMMConsumer) {
        _pLoginHandler = new LoginHandler(_pOMMConsumer, *_pEventQueue, *this, *_pComponentLogger);

    } else if(_pOMMProvider) {
        _pLoginHandler = new LoginHandler(_pOMMProvider, *_pEventQueue, *this, *_pComponentLogger);
    }
    _pLoginHandler->sendRequest(_userName, instance, appid, pos);

    // NOTE: OMM Provider doesn't need a valid login at the moment. This will be changed in future version of RMDS (ADS)
    if(_pOMMProvider) {
        _logWarning("[Pyrfa::login] OMM Provider doesn't need a valid login. (NOTE: this may change this in the future)");
        exec("time.sleep(1)");
        return;
    }

    // wait for login status
    int maxTry = 10;
    for(int ii=0; ii < maxTry; ii++) {
        exec("time.sleep(.5)");
        dispatchEventQueue(0);
        if(_pLoginHandler->receivedLoginStatus())
            break;
    }

    if(!_isConnectionUp && !_pLoginHandler->isLoggedIn()) {
        _logError("[Pyrfa::login] Connection is down. Check ServerList in config file.");
    }

    if(!_pLoginHandler->receivedLoginStatus()) {
        _logError("[Pyrfa::login] Login status has not been received.");
    }

    if(!_pLoginHandler->isLoggedIn()) {
        _log = "[Pyrfa::login] Login failed. Please check data permission. (username: ";
        _log.append(_userName.c_str());
        _log.append("; statusText: " + _pLoginHandler->getStatusText());
        _log.append(")");
        _logError(_log.c_str());
        //cleanUp();
        //exit(1);
        throw py_error(_log.c_str());
    } else {
        _log = "[Pyrfa::login] Login successful. (username: ";
        _log.append(_userName.c_str());
        _log.append(")");
        _logInfo(_log.c_str());
    }
    dispatchEventQueue(0);
}

bool Pyrfa::isLoggedIn() const {
    if(_pLoginHandler != NULL) {
        return _pLoginHandler->isLoggedIn();
    } else {
        return false;
    }
}

boost::python::tuple Pyrfa::directoryRequest() {
    if(!_pOMMConsumer)
        return boost::python::tuple();

    if(_debug)
        _logInfo("[Pyrfa::directoryRequest] Directory request... ");

    rfa::common::RFA_String fullname;
    fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
    fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\ServiceName";
    _serviceName = _pConfigDb->getConfigDb( fullname ).c_str();

    _pDirectoryHandler = new DirectoryHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, *_pComponentLogger);
    _pDirectoryHandler->setDebugMode(_debug);
    _pDirectoryHandler->sendRequest();
    exec("time.sleep(0.5)");
    _directory = boost::python::tuple();
    _directory = dispatchEventQueue(0);
    return _directory;
}

void Pyrfa::dictionaryRequest() {
    rfa::common::RFA_String downloadDataDict = "true";
    rfa::common::RFA_String dumpDataDict = "false";

    rfa::common::RFA_String fullname;

    // Lookup for UserName in configDb for current session
    fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
    fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\downloadDataDict";
    downloadDataDict = _pConfigDb->getConfigDb( fullname );
    if (downloadDataDict.empty())
        downloadDataDict = "true";

    if (downloadDataDict == "true") {
        // dumpDataDict = true or false
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\dumpDataDict";
        dumpDataDict = _pConfigDb->getConfigDb( fullname );
        if (dumpDataDict.empty())
            dumpDataDict = "false";
    } else {
        // fieldDictionaryFilename
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\fieldDictionaryFilename";
        _fieldDictionaryFilename = _pConfigDb->getConfigDb( fullname );
        // enumTypeFilename
        fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
        fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\enumTypeFilename";
        _enumTypeFilename = _pConfigDb->getConfigDb( fullname );

        // Try dictionaries from package directory
        if (_fieldDictionaryFilename.empty() || _enumTypeFilename.empty()) {
            object module = import("pyrfa");
            rfa::common::RFA_String path(extract<const char*>(module.attr("__path__")[0]));
            if (_fieldDictionaryFilename.empty())
                _fieldDictionaryFilename = path + "/RDMFieldDictionary";
            if (_enumTypeFilename.empty())
                _enumTypeFilename = path + "/enumtype.def";
        }
    }

    /*
    * Request dictionary
    * from P2PS aka network dictionary
    * or use local dictionary
    */
    if(!_pOMMConsumer && !_pOMMProvider) {
        _logError("[Pyrfa::dictionaryRequest] ERROR. No OMMconsumer or OMMprovider created.");
        return;
    }

    if (_connectionType == "RSSL_PROV" && downloadDataDict == "true") {
        _logError("[Pyrfa::dictionaryRequest] ERROR. OMMInteractiveProvider not able to download network dictionary. Use local only.");
        return;
    }

    rfa::common::UInt32 dictTraceLevel = 0;
    if (_fieldDictionaryFilename.length() && _enumTypeFilename.length()) {
        if(_debug) {
            _log = "[Pyrfa::dictionaryRequest] Using local dictionary from ";
            _log.append(_fieldDictionaryFilename);
            _log.append(" and ");
            _log.append(_enumTypeFilename);
            _logInfo(_log.c_str());
        }
        _pDictionaryHandler = new DictionaryHandler(_fieldDictionaryFilename.c_str(), _enumTypeFilename.c_str(), *_pComponentLogger);
        _pDictionaryHandler->setDebugMode(_debug);
    } else {
        if(_debug)
            _logInfo("[Pyrfa::dictionaryRequest] Downloading data dictionary from server... ");

        if(dumpDataDict == "true") {
            /*
            * RDMDict::Trace = 0x03; // dump dictionary version
            * RDMDict::Trace = 0x30; // dump dictionary to screen
            * RDMDict::Trace = 0x20; // dump fieldDictionary
            * RDMDict::Trace = 0x10; // dump enumTypedef
            */
            dictTraceLevel = 0x33;
        }
        _pDictionaryHandler = new DictionaryHandler(_pOMMConsumer, _pEventQueue, this, _serviceName, dictTraceLevel, *_pComponentLogger);
        _pDictionaryHandler->setDebugMode(_debug);
        if(_pDirectoryHandler == NULL) {
            _logError("[Pyrfa::dictionaryRequest] ERROR. Using network dict. Must invoke Directory request first!!");
            return;
        }
        // if _serviceName is up then send dictionary request
        if(_pDirectoryHandler->isServiceUp()) {
            if (!_pDictionaryHandler->isAvailable())
                _pDictionaryHandler->sendRequest();
        } else {
            _log = "[Pyrfa::dictionaryRequest] ERROR. This service is down: ";
            _log.append(_serviceName.c_str());
            _logError(_log.c_str());
        }
    }
    assert(_pDictionaryHandler);

    // wait until dictionary (field dic and enum type) are completely received
    while (!isDictionaryRefreshComplete() && (downloadDataDict == "true")) {
        dispatchEventQueue(0);
    }
    dispatchEventQueue(0);
}

bool Pyrfa::isNetworkDictionaryAvailable() const {
    if(!_pDictionaryHandler)
        return false;
    return _pDictionaryHandler->isAvailable();
}

bool Pyrfa::isDictionaryRefreshComplete() const {
    if(_pDictionaryHandler != NULL) {
        return _pDictionaryHandler->isDictionaryRefreshComplete();
    } else {
        return true;
    }
}

bool Pyrfa::isSymbolListRefreshComplete() const {
    if(_pSymbolListHandler != NULL) {
        return _pSymbolListHandler->isSymbolListRefreshComplete();
    } else {
        return true;
    }
}

bool Pyrfa::isTimeSeriesParseComplete() const {
    if(_pTimeSeriesHandler != NULL) {
        return _pTimeSeriesHandler->isTimeSeriesParseComplete();
    } else {
        return true;
    }
}

bool Pyrfa::isHistoryRefreshComplete() const {
    if(_pHistoryHandler != NULL) {
        return _pHistoryHandler->isHistoryRefreshComplete();
    } else {
        return true;
    }
}

bool Pyrfa::isConnectionUp() const {
    return _isConnectionUp;
}
void Pyrfa::symbolListRequest(object const &argv=object()) {
    if(!_pOMMConsumer)
        return;

    if(_pDictionaryHandler == NULL || _pDirectoryHandler == NULL) {
        _logError("[Pyrfa::symbolListRequest] ERROR. Must invoke Directory/Dictionary requests first!!");
        return;
    }

    std::string itemName = "";
    if(argv != object()) {
        #if PY_MAJOR_VERSION >= 3
        itemName = extract<string>(argv);
        #else
        itemName = extract<string>(str(argv).encode("utf-8"));
        #endif
    }

    rfa::common::RFA_String fullname;
    if(_pSymbolListHandler == NULL) {
        _pSymbolListHandler = new SymbolListHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pSymbolListHandler->setDebugMode(_debug);
    }

    if(_pDirectoryHandler->isServiceUp()) {
        if(itemName == "") {
            // Lookup for symbolList in configDb for current session
            fullname = "Default\\Sessions\\" +  _pSession->getName() + "\\connectionList";
            fullname = "Default\\Connections\\" + _pConfigDb->getConfigDb( fullname ) + "\\symbolList";
            itemName = _pConfigDb->getConfigDb( fullname ).c_str();
        }
        if(_debug) {
            _log = "[Pyrfa::symbolListRequest] SymbolList request for: ";
            _log.append(itemName.c_str());
            _logInfo(_log.c_str());
        }

        vector<string> itemList;
        boost::split(itemList, itemName, boost::is_any_of(","));
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            _pSymbolListHandler->sendRequest(itemList[i]);
        }
        _isSymbolListAvailable = true;
    } else {
        _log = "[Pyrfa::symbolListRequest] ERROR. This service is down: ";
        _log.append(_serviceName.c_str());
        _logError(_log.c_str());
        _isSymbolListAvailable = false;
    }
}

std::string Pyrfa::getSymbolList(object const &argv) {
    std::string symbolListname = "";
    if(argv != object()) {
        #if PY_MAJOR_VERSION >= 3
        symbolListname = extract<string>(argv);
        #else
        symbolListname = extract<string>(str(argv).encode("utf-8"));
        #endif
    }
    vector<string> itemList;
    boost::split(itemList, symbolListname, boost::is_any_of(","));
    _symbolList.clear();
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        symbolListRequest(str(itemList[i]));
        while (!isSymbolListRefreshComplete()) {
            dispatchEventQueue(0);
        }
        symbolListCloseRequest(str(itemList[i]));
        if(_isSymbolListAvailable) {
            if(!_pSymbolListHandler)
                return "";
            const std::list<std::string> *symbolList;
            symbolList = _pSymbolListHandler->getSymbolList();
            if(symbolList->size() > 0) {
                for(std::list<std::string>::const_iterator i=symbolList->begin(); (i != symbolList->end()); ++i) {
                    _symbolList.append(*i);
                    _symbolList.append(" ");
                }
            } else {
                if(_debug) {
                    _log = "[Pyrfa::getSymbolList] SymbolList returned empty for: ";
                    _log.append(itemList[i].c_str());
                    _logInfo(_log.c_str());
                }
            }
        }
    }
    return _symbolList;
}

void Pyrfa::symbolListCloseRequest(object const &argv) {
    if(_pSymbolListHandler == NULL)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string symbolListNames = extract<string>(argv);
    #else
    std::string symbolListNames = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, symbolListNames, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        _pSymbolListHandler->closeRequest(itemList[i]);
    }
}

void Pyrfa::symbolListCloseAllRequest() {
    if(!_pSymbolListHandler)
        _pSymbolListHandler->closeAllRequest();
}

std::string Pyrfa::getSymbolListWatchList() {
    if(!_pSymbolListHandler)
        return "";

    _watchList.clear();

    std::map<rfa::common::Handle*,std::string> watchList;
    watchList = _pSymbolListHandler->getWatchList();

    std::map<rfa::common::Handle*,std::string>::iterator it = watchList.begin();
    for(it = watchList.begin(); it != watchList.end(); it++) {
        // serialize into a Tcl list
        _watchList.append(it->second);
        _watchList.append(" ");
    }
    return _watchList;
}

std::string Pyrfa::getMarketPriceWatchList() {
    if(!_pMarketPriceHandler)
        return "";

    _watchList.clear();

    std::map<rfa::common::Handle*,std::string> watchList;
    watchList = _pMarketPriceHandler->getWatchList();

    std::map<rfa::common::Handle*,std::string>::iterator it = watchList.begin();
    for(it = watchList.begin(); it != watchList.end(); it++) {
        // serialize into a list
        _watchList.append(it->second);
        _watchList.append(" ");
    }
    return _watchList;
}

std::string Pyrfa::getMarketByOrderWatchList() {
    if(!_pMarketByOrderHandler)
        return "";

    _watchList.clear();

    std::map<rfa::common::Handle*,std::string> watchList;
    watchList = _pMarketByOrderHandler->getWatchList();

    std::map<rfa::common::Handle*,std::string>::iterator it = watchList.begin();
    for(it = watchList.begin(); it != watchList.end(); it++) {
        // serialize into a Tcl list
        _watchList.append(it->second);
        _watchList.append(" ");
    }
    return _watchList;
}

std::string Pyrfa::getMarketByPriceWatchList() {
    if(!_pMarketByPriceHandler)
        return "";

    _watchList.clear();

    std::map<rfa::common::Handle*,std::string> watchList;
    watchList = _pMarketByPriceHandler->getWatchList();

    std::map<rfa::common::Handle*,std::string>::iterator it = watchList.begin();
    for(it = watchList.begin(); it != watchList.end(); it++) {
        // serialize into a Tcl list
        _watchList.append(it->second);
        _watchList.append(" ");
    }
    return _watchList;
}

std::string Pyrfa::getTimeSeriesWatchList() {
    if(!_pTimeSeriesHandler)
        return "";

    _watchList.clear();

    std::map<rfa::common::Handle*,std::string> watchList;
    watchList = _pTimeSeriesHandler->getWatchList();

    std::map<rfa::common::Handle*,std::string>::iterator it = watchList.begin();
    for(it = watchList.begin(); it != watchList.end(); it++) {
        // serialize into a Tcl list
        _watchList.append(it->second);
        _watchList.append(" ");
    }
    return _watchList;
}

std::string Pyrfa::getHistoryWatchList() {
    if(!_pHistoryHandler)
        return "";

    _watchList.clear();

    std::map<rfa::common::Handle*,std::string> watchList;
    watchList = _pHistoryHandler->getWatchList();

    std::map<rfa::common::Handle*,std::string>::iterator it = watchList.begin();
    for(it = watchList.begin(); it != watchList.end(); it++) {
        // serialize into a list
        _watchList.append(it->second);
        _watchList.append(" ");
    }
    return _watchList;
}

void Pyrfa::marketByPriceRequest(object const &argv) {
    if(!_pOMMConsumer)
        return;

    if(_pMarketByPriceHandler == NULL) {
        _pMarketByPriceHandler = new MarketByPriceHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pMarketByPriceHandler->setDebugMode(_debug);
        if(_debug) {
            _log = "[Pyrfa::marketByPriceRequest] Subscribe to a service: ";
            _log.append(_serviceName.c_str());
            _logInfo(_log.c_str());
        }
    }

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            if(_interactionType == rfa::message::ReqMsg::InitialImageFlag)
                _pMarketByPriceHandler->closeRequest(itemList[i]);
            _pMarketByPriceHandler->sendRequest(itemList[i], _interactionType);
        }
    } else {
        _logError("[Pyrfa::marketByPriceRequest] Dictionary not made available or user not logged in");
    }
}

void Pyrfa::marketByPriceCloseRequest(object const &argv) {
    if(!_pMarketByPriceHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        _pMarketByPriceHandler->closeRequest(itemList[i]);
    }
}

void Pyrfa::marketByPriceCloseAllRequest() {
    if(_pMarketByPriceHandler)
        _pMarketByPriceHandler->closeAllRequest();
}


void Pyrfa::marketPriceRequest(object const &argv) {
    if(!_pOMMConsumer)
        return;

    if(_pMarketPriceHandler == NULL) {
        _pMarketPriceHandler = new MarketPriceHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pMarketPriceHandler->setDebugMode(_debug);
        if(_debug) {
            _log = "[Pyrfa::marketPriceRequest] Subscribe to a service: ";
            _log.append(_serviceName.c_str());
            _logInfo(_log.c_str());
        }
    }

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            if(_interactionType == rfa::message::ReqMsg::InitialImageFlag)
                _pMarketPriceHandler->closeRequest(itemList[i]);
            _pMarketPriceHandler->sendRequest(itemList[i], _viewFIDs, _interactionType);
        }
    } else {
        _logError("[Pyrfa::marketPriceRequest] Dictionary not made available or user not logged in");
    }
}

void Pyrfa::marketPriceCloseRequest(object const &argv) {
    if(!_pMarketPriceHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        _pMarketPriceHandler->closeRequest(itemList[i]);
    }
}

void Pyrfa::marketPriceCloseAllRequest() {
    if(_pMarketPriceHandler)
        _pMarketPriceHandler->closeAllRequest();
}

void Pyrfa::marketPricePause(object const &argv) {
    if(!_pOMMConsumer || !_pMarketPriceHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            _pMarketPriceHandler->sendRequest(itemList[i], "", rfa::message::ReqMsg::PauseFlag);
        }
    } else {
        _logError("[Pyrfa::marketPricePause] Dictionary not made available or user not logged in");
    }
    if(_debug) {
        _log = "[Pyrfa::marketPricePause] Pause subscription to: ";
        _log.append(itemName.c_str());
        _logInfo(_log.c_str());
    }
}

void Pyrfa::marketPriceResume(object const &argv) {
    if(!_pOMMConsumer || !_pMarketPriceHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            _pMarketPriceHandler->sendRequest(itemList[i], _viewFIDs, rfa::message::ReqMsg::InterestAfterRefreshFlag);
        }
    } else {
        _logError("[Pyrfa::marketPriceResume] Dictionary not made available or user not logged in");
    }
    if(_debug) {
        _log = "[Pyrfa::marketPriceResume] Resume subscription to: ";
        _log.append(itemName.c_str());
        _logInfo(_log.c_str());
    }
}


void Pyrfa::marketByOrderRequest(object const &argv) {
    if(!_pOMMConsumer)
        return;

    if(_pMarketByOrderHandler == NULL) {
        _pMarketByOrderHandler = new MarketByOrderHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pMarketByOrderHandler->setDebugMode(_debug);
        if(_debug) {
            _log = "[Pyrfa::marketByOrderRequest] Subscribe to a service: ";
            _log.append(_serviceName.c_str());
            _logInfo(_log.c_str());
        }
    }

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            if(_interactionType == rfa::message::ReqMsg::InitialImageFlag)
                _pMarketByOrderHandler->closeRequest(itemList[i]);
            _pMarketByOrderHandler->sendRequest(itemList[i], _interactionType);
        }
    } else {
        _logError("[Pyrfa::marketByOrderRequest] Dictionary not made available or user not logged in");
    }
}

void Pyrfa::marketByOrderCloseRequest(object const &argv) {
    if(!_pMarketByOrderHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        _pMarketByOrderHandler->closeRequest(itemList[i]);
    }
}

void Pyrfa::marketByOrderCloseAllRequest() {
    if(_pMarketByOrderHandler)
        _pMarketByOrderHandler->closeAllRequest();
}
/*
* Dispatch events from event queue
* return pending events on the queue.
*        or -1 if queue is inactive
*/
boost::python::tuple Pyrfa::dispatchEventQueue(long timeout=0) {
    // output string returned
    // to Python interpreter
    _out = boost::python::tuple();

    // try at least once
    long pendingEvents = 1;

    long processedEvents = 0;

    clock_t startTime = clock();
    while ( pendingEvents > 0 )
    {
        // dispatch with a xx milisecond timeout
        switch ( pendingEvents = _pEventQueue->dispatch( timeout ) ) {
            case rfa::common::Dispatchable::NothingDispatched:
                // nothing in event queue, empty the output
                _eventData = boost::python::tuple();
                _out = boost::python::tuple();
                break;
            case rfa::common::Dispatchable::NothingDispatchedInActive:
                _logError("[Pyrfa::dispatchEventQueue] Nothing dispatched - Inactive. The EventQueue has been deactivated.");
                // if the event queue has been deactivated then exit the loop
                pendingEvents = -1;
                break;
            case rfa::common::Dispatchable::NothingDispatchedPartOfGroup:
                _logError("[Pyrfa::dispatchEventQueue] Nothing dispatched - PartOfGroup. The EventQueue is part of an EventQueueGroup and should not be used directly.");
                break;

            case rfa::common::Dispatchable::NothingDispatchedNoActiveEventStreams:
                _logError("[Pyrfa::dispatchEventQueue] Nothing dispatched - NoActiveEventStreams. The EventQueue does not have any open Event Streams.");
                break;
            default:
                processedEvents++;
                if(_debug)
                    std::cout << "[Pyrfa::dispatchEventQueue] Event loop - approximate pending Events: " << pendingEvents << std::endl;
                break;
        }
        // append eventData from processEvent to Python shell
        // clear eventData after appending to avoid duplicate due to logging
        _out += _eventData;
        _eventData = boost::python::tuple();
    }

    if(_debug && (processedEvents > 0)) {
        clock_t endTime = clock();
        if(endTime > startTime)
            cout << "[Pyrfa::dispatchEventQueue] Elapsed time: " << ((float)(endTime - startTime)/CLOCKS_PER_SEC) << " secs (" << (long)(processedEvents/((float)(endTime - startTime)/CLOCKS_PER_SEC)) << " upd/sec)"<< endl;
    }

    // get messages from loggerr
    dispatchLoggerEventQueue(0);

    // return output to Python only when pendingEvents=0 (nothing in the queue)
    // So, one returned output often contains more that one update.
    return _out;
}

void Pyrfa::dispatchLoggerEventQueue(long timeout=0) {

    // try at least once
    long pendingEvents = 1;

    long processedEvents = 0;

    clock_t startTime = clock();
    while ( pendingEvents > 0 )
    {
        // dispatch with a xx milisecond timeout
        switch ( pendingEvents = _pLoggerEventQueue->dispatch( timeout ) ) {
            case rfa::common::Dispatchable::NothingDispatched:
                break;
            case rfa::common::Dispatchable::NothingDispatchedInActive:
                _logError("[Pyrfa::dispatchLoggerEventQueue] Nothing dispatched - Inactive. The EventQueue has been deactivated.");
                // if the event queue has been deactivated then exit the loop
                pendingEvents = -1;
                break;
            case rfa::common::Dispatchable::NothingDispatchedPartOfGroup:
                _logError("[Pyrfa::dispatchLoggerEventQueue] Nothing dispatched - PartOfGroup. The EventQueue is part of an EventQueueGroup and should not be used directly.");
                break;

            case rfa::common::Dispatchable::NothingDispatchedNoActiveEventStreams:
                _logError("[Pyrfa::dispatchLoggerEventQueue] Nothing dispatched - NoActiveEventStreams. The EventQueue does not have any open Event Streams.");
                break;
            default:
                processedEvents++;
                if(_debug)
                    std::cout << "[Pyrfa::dispatchLoggerEventQueue] Event loop - approximate pending Events: " << pendingEvents << std::endl;
                break;
        }
    }

    if(_debug && (processedEvents > 0)) {
        clock_t endTime = clock();
        if(endTime > startTime)
            cout << "[Pyrfa::dispatchLoggerEventQueue] Elapsed time: " << ((float)(endTime - startTime)/CLOCKS_PER_SEC) << " secs (" << (long)(processedEvents/((float)(endTime - startTime)/CLOCKS_PER_SEC)) << " upd/sec)"<< endl;
    }
}



void Pyrfa::deactivateEventQueue() {
    if(_pEventQueue)
        _pEventQueue->deactivate();
    if(_pLoggerEventQueue)
        _pLoggerEventQueue->deactivate();
}

void Pyrfa::cleanUp() {
    if(_debug) {
        _log = "[Pyrfa::cleanUp]";
        _logInfo(_log.c_str());
    }

    // stop sending events to event queue
    deactivateEventQueue();

    // logs out application
    if(_pLoginHandler) {
        _pLoginHandler->cleanup();
        delete _pLoginHandler;
        _pLoginHandler = 0;
    }

    if(_pDirectoryHandler) {
        delete _pDirectoryHandler;
        _pDirectoryHandler = 0;
    }

    if(_pOMMCProvServer) {
        _pOMMCProvServer->closeAllSubmit();
        delete _pOMMCProvServer;
        _pOMMCProvServer = 0;
    }

    if(_pOMMInteractiveProvider) {
        //_pOMMInteractiveProvider->closeAllSubmit();
        delete _pOMMInteractiveProvider;
        _pOMMInteractiveProvider = 0;
    }

    // close all opening items
    if(!getSymbolListWatchList().empty()) {
        symbolListCloseAllRequest();
    }
    if(!getMarketPriceWatchList().empty()) {
        marketPriceCloseAllRequest();
    }
    if(!getMarketByOrderWatchList().empty()) {
        marketByOrderCloseAllRequest();
    }
    if(!getMarketByPriceWatchList().empty()) {
        marketByPriceCloseAllRequest();
    }

    if(!getTimeSeriesWatchList().empty()) {
        timeSeriesCloseAllRequest();
    }
    if(!getHistoryWatchList().empty()) {
        historyCloseAllRequest();
    }

    // Unregister Logger Client and release application logger
    if(_pComponentLogger) {
        _pComponentLogger->destroy();
        _pComponentLogger = 0;
    }
    if( _pLogger) {
        _pLogger->release();
        _pLogger = 0;
    }
    if (_pLoggerClient) {
        delete _pLoggerClient;
        _pLoggerClient = 0;
    }

    // destroy EventQueue
    if (_pEventQueue) {
        _pEventQueue->destroy();
        _pEventQueue = 0;
    }
    if (_pLoggerEventQueue) {
        _pLoggerEventQueue->destroy();
        _pLoggerEventQueue = 0;
    }

    // destroy event source
    if(_pOMMConsumer) {
        if (_pConnHandle) {
            _pOMMConsumer->unregisterClient(_pConnHandle);
            _pConnHandle = 0;
        }
        if (_pErrHandle) {
            _pOMMConsumer->unregisterClient(_pErrHandle);
            _pErrHandle = 0;
        }
        _pOMMConsumer->destroy();
        _pOMMConsumer = 0;
    }
    if(_pOMMProvider) {
        if (_pConnHandle) {
            _pOMMProvider->unregisterClient(_pConnHandle);
            _pConnHandle = 0;
        }
        if (_pClientSessListHandle) {
            _pOMMProvider->unregisterClient(_pClientSessListHandle);
            _pClientSessListHandle = 0;
        }
        if (_pListConnHandle) {
            _pOMMProvider->unregisterClient(_pListConnHandle);
            _pListConnHandle = 0;
        }
        if (_pErrHandle) {
            _pOMMProvider->unregisterClient(_pErrHandle);
            _pErrHandle = 0;
        }
        _pOMMProvider->destroy();
        _pOMMProvider = 0;
    }

    if(_pSymbolListHandler) {
        delete _pSymbolListHandler;
        _pSymbolListHandler = 0;
    }

    if(_pMarketPriceHandler) {
        delete _pMarketPriceHandler;
        _pMarketPriceHandler = 0;
    }

    if(_pMarketByOrderHandler) {
        delete _pMarketByOrderHandler;
        _pMarketByOrderHandler = 0;
    }

    if(_pMarketByPriceHandler) {
        delete _pMarketByPriceHandler;
        _pMarketByPriceHandler = 0;
    }

    if(_pTimeSeries) {
        _pTimeSeries->destroy();
        _pTimeSeries = 0;
    }

    if(_pTS1DictDb) {
        _pTS1DictDb->destroy();
        _pTS1DictDb = 0;
    }

    if(_pHistoryHandler) {
        delete _pHistoryHandler;
        _pHistoryHandler = 0;
    }

    if(_pOMMPost) {
        delete _pOMMPost;
        _pOMMPost = 0;
    }

    if(_pDictionaryHandler) {
        delete _pDictionaryHandler;
        _pDictionaryHandler = 0;
    }

    // Release session
    if (_pSession) {
        _pSession->release();
        _pSession = 0;
    }

    // delete ConfigDb
    if (_pConfigDb) {
        delete _pConfigDb;
        _pConfigDb = 0;
    }

    // uninitialize RFA
    uninitializeRFA();
}

void Pyrfa::setDebugMode(object const &argv=object()) {
    if (!_pConfigDb)
        return;

    std::string debug = "";
    // first argument is true/false, if empty read value from config database
    if(argv != object()) {
        _debug = extract<bool>(argv);
    } else {
        debug = _pConfigDb->getConfigDb( "\\Default\\pyrfa\\debug" ).c_str();
        if(debug == "true") {
            _debug = true;
        } else {
            _debug = false;
        }
    }
    _pConfigDb->setDebugMode(_debug);
}

/* External logger with object input */
void Pyrfa::logInfo(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* log = extract<const char*>(argv);
    #else
    const char* log = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    if(_pComponentLogger) {
        _pComponentLogger->log(LM_GENERIC_ONE,rfa::common::Information, log);
        dispatchLoggerEventQueue(0);
    }
}

void Pyrfa::logWarning(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* log = extract<const char*>(argv);
    #else
    const char* log = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    if(_pComponentLogger) {
        _pComponentLogger->log(LM_GENERIC_ONE,rfa::common::Warning, log);
        dispatchLoggerEventQueue(0);
    }
}

void Pyrfa::logError(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* log = extract<const char*>(argv);
    #else
    const char* log = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    if(_pComponentLogger) {
        _pComponentLogger->log(LM_UNEXPECTED_ONE,rfa::common::Error, log);
        dispatchLoggerEventQueue(0);
    }
}

/* Internal logger with std::string */
void Pyrfa::_logInfo(std::string log) {
    if(_pComponentLogger) {
        _pComponentLogger->log(LM_GENERIC_ONE,rfa::common::Information, log.c_str());
        dispatchLoggerEventQueue(0);
    }
}

void Pyrfa::_logWarning(std::string log) {
    if(_pComponentLogger) {
        _pComponentLogger->log(LM_GENERIC_ONE,rfa::common::Warning, log.c_str());
        dispatchLoggerEventQueue(0);
    }
}

void Pyrfa::_logError(std::string log) {
    if(_pComponentLogger) {
        _pComponentLogger->log(LM_UNEXPECTED_ONE,rfa::common::Error, log.c_str());
        dispatchLoggerEventQueue(0);
    }
}

void Pyrfa::processEvent(const rfa::common::Event& event){
    if(_debug)
        cout << "[Pyrfa::processEvent] @" << boost::posix_time::to_simple_string(boost::posix_time::microsec_clock::local_time().time_of_day()).c_str() << endl;

    switch (event.getType()) {
        case rfa::logger::LoggerNotifyEventEnum:
            {
                const rfa::logger::LoggerNotifyEvent &loggerEvent = static_cast<const rfa::logger::LoggerNotifyEvent &>(event);
                if(_debug) {
                    _log = "[Pyrfa::processEvent] LoggerNotifyEvent from ";
                    _log.append(loggerEvent.getComponentName().c_str());
                    _log += "(";
                    _log.append((rfa::common::Int32)loggerEvent.getSeverity());
                    _log += "):";
                    _log.append(loggerEvent.getMessageText().c_str());
                    _logInfo(_log.c_str());
                }
            }
            break;
        case rfa::sessionLayer::ConnectionEventEnum:
            {
                const rfa::sessionLayer::ConnectionEvent & CEvent = static_cast<const rfa::sessionLayer::ConnectionEvent&>(event);
                processConnectionEvent(CEvent);
            }
            break;
        case rfa::sessionLayer::OMMItemEventEnum:
            {
                const rfa::sessionLayer::OMMItemEvent & OMMEvent = static_cast<const rfa::sessionLayer::OMMItemEvent&>(event);
                processOMMItemEvent(OMMEvent);
            }
            break;
        case rfa::sessionLayer::OMMCmdErrorEventEnum:
            {
                const rfa::sessionLayer::OMMCmdErrorEvent & CmdEvent = static_cast<const rfa::sessionLayer::OMMCmdErrorEvent&>(event);
                processOMMCmdErrorEvent(CmdEvent);
            }
            break;
        case rfa::sessionLayer::OMMActiveClientSessionEventEnum:
            processOMMActiveClientSessionEvent(static_cast<const rfa::sessionLayer::OMMActiveClientSessionEvent&>(event));
			break;
		case rfa::sessionLayer::OMMInactiveClientSessionEventEnum:
			processOMMInactiveClientSessionEvent(static_cast<const rfa::sessionLayer::OMMInactiveClientSessionEvent&>(event));
			break;
        case rfa::sessionLayer::OMMSolicitedItemEventEnum:
			processOMMSolicitedItemEvent(static_cast<const rfa::sessionLayer::OMMSolicitedItemEvent&>(event));
			break;
        default:
            {
                _log = "[Pyrfa::processEvent] Event type <";
                _log.append(event.getType());
                _log += "> not handled!! ";
                _logError(_log.c_str());
            }
        break;
    }
}

void Pyrfa::processConnectionEvent(const rfa::sessionLayer::ConnectionEvent & CEvent) {
    const rfa::sessionLayer::ConnectionStatus& status = CEvent.getStatus();
    if (status.getState() == rfa::sessionLayer::ConnectionStatus::Up) {
        if(_debug) {
            _log = "[Pyrfa::processConnectionEvent] Connection Up!";
            _logInfo(_log.c_str());
        }
        _isConnectionUp = true;
    } else {
        if(_debug) {
            _log = "[Pyrfa::processConnectionEvent] Connection Down!!!!";
            _logError(_log.c_str());
        }
        _isConnectionUp = false;

        // remove published itemList
        if(_pOMMCProvServer != NULL) {
            _pOMMCProvServer->clearPublishedItemList();
        }
    }
}

void Pyrfa::processOMMActiveClientSessionEvent(const rfa::sessionLayer::OMMActiveClientSessionEvent& event) {
    _log = "[Pyrfa::processOMMActiveClientSessionEvent] Received an active client session request.";
    _logInfo(_log.c_str());

    const rfa::common::Handle* pCliSessHandle = event.getClientSessionHandle();
    rfa::sessionLayer::OMMClientSessionIntSpec cliSessInterestSpec;
    cliSessInterestSpec.setClientSessionHandle(pCliSessHandle);

    rfa::common::Handle* tmpSessHandle = _pOMMProvider->registerClient(_pEventQueue, &cliSessInterestSpec, *this);
    assert(tmpSessHandle == pCliSessHandle); //The returned handle should be the exact same handle as the pClientSessHandle

    _log = "[Pyrfa::processOMMActiveClientSessionEvent] Client session has been accepted.";
    _logInfo(_log.c_str());

    if(_pOMMInteractiveProvider == NULL) {
        _pOMMInteractiveProvider = new OMMInteractiveProvider(_pOMMProvider, _vendorName, _serviceName, _fieldDictionaryFilename, _enumTypeFilename, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pOMMInteractiveProvider->setDebugMode(_debug);
    }

    _pOMMInteractiveProvider->addClientWatchList(pCliSessHandle);
    dispatchEventQueue(0);
}

void Pyrfa::processOMMInactiveClientSessionEvent(const rfa::sessionLayer::OMMInactiveClientSessionEvent& event) {
    // output string meant for Python tuple
    _eventData = boost::python::tuple();

    _log = "[Pyrfa::processOMMInactiveClientSessionEvent] Client session disconnected.";
    _logInfo(_log.c_str());

	const rfa::common::Handle* pCSH = event.getHandle();

    _pOMMInteractiveProvider->removeClientWatchList(pCSH, _eventData);
}

void Pyrfa::processOMMSolicitedItemEvent(const rfa::sessionLayer::OMMSolicitedItemEvent& event) {
    // output string meant for Python tuple
    _eventData = boost::python::tuple();

    const rfa::common::Msg& msg = event.getMsg();
	switch(msg.getMsgType())
	{
		case rfa::message::ReqMsgEnum:
            _pOMMInteractiveProvider->processReqMsg(event, _eventData);
			break;
        case rfa::message::PostMsgEnum:
            _pOMMInteractiveProvider->processPostMsg(event);
			break;
		default:
            _log = "[Pyrfa::processOMMSolicitedItemEvent] Event message type <";
            _log.append(msg.getMsgType());
            _log += "> not handled!! ";
            _logError(_log.c_str());
	}
}

void Pyrfa::setServiceName(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    std::string serviceName = extract<string>(argv);
    #else
    std::string serviceName = extract<string>(str(argv).encode("utf-8"));
    #endif
    _serviceName = serviceName;
}

std::string Pyrfa::getServiceName() {
    return _serviceName;
}

void Pyrfa::timeSeriesRequest(object const &argv) {
    if(!_pOMMConsumer)
        return;

    // If timeseries is called before, destroy the old one first
    if(_pTimeSeries) {
        _pTimeSeries->destroy();
        _pTimeSeries = 0;
    }

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        boost::trim(itemName);
        if(itemName.empty())
            return;
        // Create a new TS1Series object
        _pTimeSeries = rfa::ts1::TS1Series::createSeries(itemName.c_str(), _timeSeriesPeriod);

        if(!_pTimeSeriesHandler) {
            // Initiate TS1 DB object for once
            _pTS1DictDb = rfa::ts1::TS1DefDb::createTS1DefDb();
            _pTimeSeriesHandler = new TimeSeriesHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, _pDictionaryHandler->getDictionary(), *_pComponentLogger, *_pTS1DictDb);
            _pTimeSeriesHandler->initTimeSeries(_pTimeSeries);
            _pTimeSeriesHandler->setDebugMode(_debug);

            // Request the TS1 Dictionary
            if(_debug) {
                _log = "[Pyrfa::timeSeriesRequest] TS1 DB RICs from ";
                _log.append(_serviceName.c_str());
                _logInfo(_log.c_str());
            }
            for(int i=0; i < _pTS1DictDb->getNumberDbRics(); ++i ) {
                _pTimeSeriesHandler->sendRequest(_pTS1DictDb->getTs1DbRics(i));
            }
            exec("time.sleep(0.1)");
        } else {
            _pTimeSeriesHandler->initTimeSeries(_pTimeSeries);
        }

        // Request the TS1 Data.
        _pTimeSeriesHandler->sendRequest(_pTimeSeries->getPrimaryRic());
    } else {
        _logError("[Pyrfa::timeSeriesRequest] Dictionary not made available or user not logged in");
    }
}

void Pyrfa::timeSeriesCloseRequest(object const &argv) {
    if(!_pTimeSeriesHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    boost::trim(itemName);
    _pTimeSeriesHandler->closeRequest(itemName);
}

void Pyrfa::timeSeriesCloseAllRequest() {
    if(_pTimeSeriesHandler)
        _pTimeSeriesHandler->closeAllRequest();
}

void Pyrfa::setTimeSeriesPeriod(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    std::string period = extract<string>(argv);
    #else
    std::string period = extract<string>(str(argv).encode("utf-8"));
    #endif
    if ( STRICMP( period.c_str(), "Daily") == 0 || STRICMP( period.c_str(), "daily") == 0 )
        _timeSeriesPeriod = rfa::ts1::TS1Series::Daily;
    else if ( STRICMP( period.c_str(), "Weekly") == 0 || STRICMP( period.c_str(), "weekly") == 0 )
        _timeSeriesPeriod = rfa::ts1::TS1Series::Weekly;
    else if ( STRICMP( period.c_str(), "Monthly") == 0 || STRICMP( period.c_str(), "monthly") == 0 )
        _timeSeriesPeriod = rfa::ts1::TS1Series::Monthly;
    else
        _timeSeriesPeriod = rfa::ts1::TS1Series::Daily;
}

void Pyrfa::setTimeSeriesMaxRecords(int maxRecords) {
    _timeSeriesMaxRecords = maxRecords;
}

boost::python::tuple Pyrfa::getTimeSeries(object const &argv) {
    // subscribe, wait for completion and unsubscribe add DB rics and data ric
    // getTimeSeries supports only one RIC series retreival at a time
    // #if PY_MAJOR_VERSION >= 3
    // std::string itemName = extract<string>(argv);
    // #else
    // std::string itemName = extract<string>(str(argv).encode("utf-8"));
    // #endif
    // timeSeriesRequest(str(itemName));
    timeSeriesRequest(argv);
    while (!isTimeSeriesParseComplete()) {
        dispatchEventQueue(100);
    }
    // timeSeriesCloseRequest(str(itemName));
    timeSeriesCloseRequest(argv);

    if(!_pTimeSeriesHandler)
        return boost::python::tuple();

    const std::list<std::string> *timeSeries;
    timeSeries = _pTimeSeriesHandler->getTimeSeries(_timeSeriesMaxRecords);
    rfa::common::UInt32 timeSeriesRecordCount = timeSeries->size();
    if(timeSeriesRecordCount > 0) {
        rfa::common::UInt32 ii(0);
        _timeSeries = boost::python::tuple();
        for(std::list<std::string>::const_iterator i=timeSeries->begin(); i != timeSeries->end(); ++i) {
            if(_debug)
                cout << *i << endl;
            // serialize into a Python tuple
            _timeSeries += boost::python::make_tuple(*i);
            ii++;
        }
        return _timeSeries;
    } else {
        if(_debug)
            _logInfo("[Pyrfa::getTimeSeries] Timeseries is empty.");
        return boost::python::tuple();
    }
}

void Pyrfa::historyRequest(object const &argv) {
    if(!_pOMMConsumer)
        return;

    if(_pHistoryHandler == NULL) {
        _pHistoryHandler = new HistoryHandler(_pOMMConsumer, *_pEventQueue, *this, _serviceName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pHistoryHandler->setDebugMode(_debug);
        if(_debug) {
            _log = "[Pyrfa::historyRequest] Subscribe to a service: ";
            _log.append(_serviceName.c_str());
            _logInfo(_log.c_str());
        }
    }

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        for(size_t i = 0; i < itemList.size(); i++) {
            boost::trim(itemList[i]);
            if(itemList[i].empty())
                continue;
            if(_interactionType == rfa::message::ReqMsg::InitialImageFlag)
                _pHistoryHandler->closeRequest(itemList[i]);
            _pHistoryHandler->sendRequest(itemList[i], _interactionType);
        }
        _isHistoryAvailable = true;
    } else {
        _logError("[Pyrfa::historyRequest] Dictionary not made available or user not logged in");
        _isHistoryAvailable = false;
    }
}

void Pyrfa::historyCloseRequest(object const &argv) {
    if(!_pHistoryHandler)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv);
    #else
    std::string itemName = extract<string>(str(argv).encode("utf-8"));
    #endif
    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        _pHistoryHandler->closeRequest(itemList[i]);
    }
}

void Pyrfa::historyCloseAllRequest() {
    if(_pHistoryHandler)
        _pHistoryHandler->closeAllRequest();
}

void Pyrfa::directorySubmit(object const &argv1=object(), object const &argv2=object()) {
    if(!_pOMMProvider) {
        _logError("[Pyrfa::directorySubmit] ERROR. No OMMprovider created.");
        return;
    }

    if(_pDictionaryHandler == NULL) {
        _logError("[Pyrfa::directorySubmit] ERROR. Must load local dictionary first.");
        return;
    }

    /*
    * Non-interactive OMM publisher does not require login.
    * This may change in the future (by both RMDS and RFA)
    */
    if(!_pLoginHandler->isLoggedIn()) {
        _logError("[Pyrfa::directorySubmit] Not logged in. Abort.");
        return;
    }

    if(_pOMMCProvServer == NULL) {
        _pOMMCProvServer = new OMMCProvServer(_pOMMProvider, _pLoginHandler->_pLoginHandle, _vendorName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
        _pOMMCProvServer->setDebugMode(_debug);
    }

    /*
    / submit directory with domain type (capability)
    * currently supports:
    * 6  - market price
    * 7  - market by order
    * 8  - market by price
    * 10 - symbol list
    * 12 - history (this is not an official RDM)
    */
    std::string domainType = "";
    if( argv1 != object()) {
        #if PY_MAJOR_VERSION >= 3
        domainType = extract<string>(argv1);
        #else
        domainType = extract<string>(str(argv1).encode("utf-8"));
        #endif
    }
    std::string service = "";
    if (argv2 != object()) {
        #if PY_MAJOR_VERSION >= 3
        service = extract<string>(argv2);
        #else
        service = extract<string>(str(argv2).encode("utf-8"));
        #endif
    }

    rfa::common::RFA_String serviceName(service.c_str());
    if(serviceName.empty()) {
        serviceName = _serviceName.c_str();
    }
    if(!domainType.empty()) {
        vector<string> domainList;
        boost::split(domainList, domainType, boost::is_any_of(","));
        std::string domain;
        for(size_t i = 0; i < domainList.size(); i++) {
            boost::trim(domainList[i]);
            try {
                _pOMMCProvServer->directorySubmit(boost::lexical_cast<int>(domainList[i]), serviceName);
            } catch (exception& e) {
                _log = "[Pyrfa::directorySubmit] ERROR submitting domain: ";
                _log.append(domainList[i].c_str());
                _logError(_log.c_str());
                continue;
            }
        }
    } else {
        _pOMMCProvServer->directorySubmit(0, serviceName);
    }
    dispatchEventQueue(500);
}

/*
* Provider
*/
void Pyrfa::marketPriceSubmit(object const &argv) {
    dispatchEventQueue(0);
    if(!_pOMMProvider) {
        _log = "[Pyrfa::marketPriceSubmit] ERROR. No OMMprovider created.";
        _logError(_log.c_str());
        return;
    }

    if(_pDictionaryHandler == NULL && (_connectionType != "RSSL_PROV")) {
        _log = "[Pyrfa::marketPriceSubmit] ERROR. Must request or load dictionary first.";
        _logError(_log.c_str());
        return;
    }

    std::string symbolName;
    std::string mtype;
    rfa::common::RFA_String serviceName;
    rfa::common::RFA_Vector<rfa::common::RFA_String> fieldList;
    std::string session;

    // if argv is a dict then make it to tuple
    extract<dict> isDict(argv);
    boost::python::tuple marketpriceTuple;
    if (isDict.check()) {
        marketpriceTuple = boost::python::make_tuple(argv);
    } else {
        marketpriceTuple = extract<boost::python::tuple>(argv);
    }

    for (int ii = 0 ; ii < len(marketpriceTuple); ii++) {
        fieldList.clear();
        mtype = "update";
        symbolName = "";
        serviceName = _serviceName.c_str();
        session = "";

        dict dictElement = extract<dict>(marketpriceTuple[ii]);
        boost::python::list keys = (boost::python::list)dictElement.keys();

        //Because the dictionary key is put in the reversed order ,
        //we need to make a loop from the last index to keep the correct sequence.
        if(_debug)
            cout << "[Pyrfa::marketPriceSubmit] fieldList: ";

        for (int j = len(keys) - 1 ; j >= 0; j--) {
            string key = extract<string>(keys[j]);
            string value = extract<string>(str(dictElement[keys[j]]));
            if(_debug) {
                cout << key.c_str() << "=" << value.c_str();
                if (j != 0 ) {
                    cout << ",";
                }
            }
            // Exclude RIC key as not defined in RDM dictionary
            if (key == "RIC") {
                symbolName = value;
                continue;
            } else if (key == "MTYPE") {
                mtype = value;
                boost::to_lower(mtype);
                continue;
            } else if (key == "SERVICE") {
                serviceName = value.c_str();
                continue;
            } else if (key == "SESSIONID") {
                session = value;
                continue;
            }
            // construct fieldList for the given symbolName
            fieldList.push_back(rfa::common::RFA_String(key.c_str()));
            fieldList.push_back(rfa::common::RFA_String(value.c_str()));
        }
        if(_debug)
            cout << endl;

        if (_connectionType == "RSSL_PROV") {
            if (_isConnectionUp && _pOMMInteractiveProvider)
                _pOMMInteractiveProvider->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_MARKET_PRICE, mtype, serviceName, session);
        } else {
            if(_pOMMCProvServer == NULL) {
                _pOMMCProvServer = new OMMCProvServer(_pOMMProvider, _pLoginHandler->_pLoginHandle, _vendorName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
                _pOMMCProvServer->setDebugMode(_debug);
            }
            if(_pLoginHandler->isLoggedIn() && _isConnectionUp)
                _pOMMCProvServer->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_MARKET_PRICE, mtype, serviceName);
        }
    }
}

void Pyrfa::marketByOrderSubmit(object const &argv) {
    dispatchEventQueue(0);
    if(!_pOMMProvider) {
        _log = "[Pyrfa::marketByOrderSubmit] ERROR. No OMMprovider created.";
        _logError(_log.c_str());
        return;
    }

    if(_pDictionaryHandler == NULL) {
        _log = "[Pyrfa::marketByOrderSubmit] ERROR. Must load local dictionary first.";
        _logError(_log.c_str());
        return;
    }

    std::string symbolName;
    std::string mtype;
    rfa::common::RFA_String serviceName;
    std::string mapAction;
    std::string mapKey;
    rfa::common::RFA_Vector<rfa::common::RFA_String> fieldList;

    // if argv is a dict then make it to tuple
    extract<dict> isDict(argv);
    boost::python::tuple marketByOrderTuple;
    if (isDict.check()) {
        marketByOrderTuple = boost::python::make_tuple(argv);
    } else {
        marketByOrderTuple = extract<boost::python::tuple>(argv);
    }

    // parse each update per symbolName
    // construct fieldList and submit
    for (int ii = 0 ; ii < len(marketByOrderTuple); ii++) {
        fieldList.clear();
        mtype = "update";
        symbolName = "";
        serviceName = _serviceName.c_str();

        dict dictElement = extract<dict>(marketByOrderTuple[ii]);
        boost::python::list keys = (boost::python::list)dictElement.keys();

        //Because the dictionary key is put in the reversed order ,
        //we need to make a loop from the last index to keep the correct sequence.
        if(_debug)
            cout << "[Pyrfa::marketByOrderSubmit] fieldList: ";

        for (int j = len(keys) - 1 ; j >= 0; j--) {
            string key = extract<string>(keys[j]);
            string value = extract<string>(str(dictElement[keys[j]]));
            if(_debug) {
                cout << key.c_str() << "=" << value.c_str();
                if (j != 0 ) {
                    cout << ",";
                }
            }
            // Exclude RIC, KEY and ACTION key as not defined in RDM dictionary
            if (key == "RIC") {
                symbolName = value;
                continue;
            } else if (key == "MTYPE") {
                mtype = value;
                boost::to_lower(mtype);
                continue;
            } else if (key == "SERVICE") {
                serviceName = value.c_str();
                continue;
            } else if (key == "ACTION") {
                mapAction = value;
                boost::to_lower(mapAction);
                continue;
            } else if (key == "KEY") {
                mapKey = value;
                continue;
            }
            // construct fieldList for the given symbolName
            fieldList.push_back(rfa::common::RFA_String(key.c_str()));
            fieldList.push_back(rfa::common::RFA_String(value.c_str()));
        }
        if(_debug)
            cout << endl;

        // submit
        if(_pOMMCProvServer == NULL) {
            _pOMMCProvServer = new OMMCProvServer(_pOMMProvider, _pLoginHandler->_pLoginHandle, _vendorName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
            _pOMMCProvServer->setDebugMode(_debug);
        }

        // submit image/update
        if(_pLoginHandler->isLoggedIn() && _isConnectionUp)
            _pOMMCProvServer->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_MARKET_BY_ORDER, mtype, serviceName, mapAction, mapKey);
    }
}

void Pyrfa::marketByPriceSubmit(object const &argv) {
    dispatchEventQueue(0);
    if(!_pOMMProvider) {
        _log = "[Pyrfa::marketByPriceSubmit] ERROR. No OMMprovider created.";
        _logError(_log.c_str());
        return;
    }

    if(_pDictionaryHandler == NULL) {
        _log = "[Pyrfa::marketByPriceSubmit] ERROR. Must load local dictionary first.";
        _logError(_log.c_str());
        return;
    }

    std::string symbolName;
    std::string mtype;
    rfa::common::RFA_String serviceName;
    std::string mapAction;
    std::string mapKey;
    rfa::common::RFA_Vector<rfa::common::RFA_String> fieldList;

    // if argv is a dict then make it to tuple
    extract<dict> isDict(argv);
    boost::python::tuple marketByPriceTuple;
    if (isDict.check()) {
        marketByPriceTuple = boost::python::make_tuple(argv);
    } else {
        marketByPriceTuple = extract<boost::python::tuple>(argv);
    }

    // parse each update per symbolName
    // construct fieldList and submit
    for (int ii = 0 ; ii < len(marketByPriceTuple); ii++) {
        fieldList.clear();
        mtype = "update";
        symbolName = "";
        serviceName = _serviceName.c_str();

        dict dictElement = extract<dict>(marketByPriceTuple[ii]);
        boost::python::list keys = (boost::python::list)dictElement.keys();

        //Because the dictionary key is put in the reversed order ,
        //we need to make a loop from the last index to keep the correct sequence.
        if(_debug)
            cout << "[Pyrfa::marketByPriceSubmit] fieldList: ";

        for (int j = len(keys) - 1 ; j >= 0; j--) {
            string key = extract<string>(keys[j]);
            string value = extract<string>(str(dictElement[keys[j]]));
            if(_debug) {
                cout << key.c_str() << "=" << value.c_str();
                if (j != 0 ) {
                    cout << ",";
                }
            }
            // Exclude RIC, KEY and ACTION key as not defined in RDM dictionary
            if (key == "RIC") {
                symbolName = value;
                continue;
            } else if (key == "MTYPE") {
                mtype = value;
                boost::to_lower(mtype);
                continue;
            } else if (key == "SERVICE") {
                serviceName = value.c_str();
                continue;
            } else if (key == "ACTION") {
                mapAction = value;
                boost::to_lower(mapAction);
                continue;
            } else if (key == "KEY") {
                mapKey = value;
                continue;
            }
            // construct fieldList for the given symbolName
            fieldList.push_back(rfa::common::RFA_String(key.c_str()));
            fieldList.push_back(rfa::common::RFA_String(value.c_str()));
        }
        if(_debug)
            cout << endl;

        // submit
        if(_pOMMCProvServer == NULL) {
            _pOMMCProvServer = new OMMCProvServer(_pOMMProvider, _pLoginHandler->_pLoginHandle, _vendorName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
            _pOMMCProvServer->setDebugMode(_debug);
        }

        // submit image/update
        if(_pLoginHandler->isLoggedIn() && _isConnectionUp)
            _pOMMCProvServer->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_MARKET_BY_PRICE, mtype, serviceName, mapAction, mapKey);
    }
}

void Pyrfa::symbolListSubmit(object const &argv) {
    dispatchEventQueue(0);
    if(!_pOMMProvider) {
        _log = "[Pyrfa::symbolListSubmit] ERROR. No OMMprovider created.";
        _logError(_log.c_str());
        return;
    }

    if(_pDictionaryHandler == NULL) {
        _log = "[Pyrfa::symbolListSubmit] ERROR. Must load local dictionary first.";
        _logError(_log.c_str());
        return;
    }

    std::string symbolName;
    std::string mtype;
    rfa::common::RFA_String serviceName;
    std::string mapAction;
    std::string mapKey;
    rfa::common::RFA_Vector<rfa::common::RFA_String> fieldList;

    // if argv is a dict then make it to tuple
    extract<dict> isDict(argv);
    boost::python::tuple symbolListTuple;
    if (isDict.check()) {
        symbolListTuple = boost::python::make_tuple(argv);
    } else {
        symbolListTuple = extract<boost::python::tuple>(argv);
    }

    // parse each update per symbolName
    // construct fieldList and submit
    for (int ii = 0 ; ii < len(symbolListTuple); ii++) {
        fieldList.clear();
        mtype = "update";
        symbolName = "";
        serviceName = _serviceName.c_str();

        dict dictElement = extract<dict>(symbolListTuple[ii]);
        boost::python::list keys = (boost::python::list)dictElement.keys();

        //Because the dictionary key is put in the reversed order ,
        //we need to make a loop from the last index to keep the correct sequence.
        if(_debug)
            cout << "[Pyrfa::symbolListSubmit] fieldList: ";

        for (int j = len(keys) - 1 ; j >= 0; j--) {
            string key = extract<string>(keys[j]);
            string value = extract<string>(str(dictElement[keys[j]]));
            if(_debug) {
                cout << key.c_str() << "=" << value.c_str();
                if (j != 0 ) {
                    cout << ",";
                }
            }
            // Exclude RIC, KEY and ACTION key as not defined in RDM dictionary
            if (key == "RIC") {
                symbolName = value;
                continue;
            } else if (key == "MTYPE") {
                mtype = value;
                boost::to_lower(mtype);
                continue;
            } else if (key == "SERVICE") {
                serviceName = value.c_str();
                continue;
            } else if (key == "ACTION") {
                mapAction = value;
                boost::to_lower(mapAction);
                continue;
            } else if (key == "KEY") {
                mapKey = value;
                continue;
            }
            // construct fieldList for the given symbolName
            fieldList.push_back(rfa::common::RFA_String(key.c_str()));
            fieldList.push_back(rfa::common::RFA_String(value.c_str()));
        }
        if(_debug)
            cout << endl;

        // submit
        if(_pOMMCProvServer == NULL) {
            _pOMMCProvServer = new OMMCProvServer(_pOMMProvider, _pLoginHandler->_pLoginHandle, _vendorName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
            _pOMMCProvServer->setDebugMode(_debug);
        }

        // submit image/update
        if(_pLoginHandler->isLoggedIn() && _isConnectionUp)
            _pOMMCProvServer->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_SYMBOL_LIST, mtype, serviceName, mapAction, mapKey);
    }
}

void Pyrfa::historySubmit(object const &argv) {
    dispatchEventQueue(0);
    if(!_pOMMProvider) {
        _log = "[Pyrfa::historySubmit] ERROR. No OMMprovider created.";
        _logError(_log.c_str());
        return;
    }

    if(_pDictionaryHandler == NULL) {
        _log = "[Pyrfa::historySubmit] ERROR. Must load local dictionary first.";
        _logError(_log.c_str());
        return;
    }

    std::string symbolName;
    std::string mtype;
    rfa::common::RFA_String serviceName;
    rfa::common::RFA_Vector<rfa::common::RFA_String> fieldList;

    // if argv is a dict then make it to tuple
    extract<dict> isDict(argv);
    boost::python::tuple historyTuple;
    if (isDict.check()) {
        historyTuple = boost::python::make_tuple(argv);
    } else {
        historyTuple = extract<boost::python::tuple>(argv);
    }

    for (int ii = 0 ; ii < len(historyTuple); ii++) {
        fieldList.clear();
        mtype = "update";
        symbolName = "";
        serviceName = _serviceName.c_str();

        dict dictElement = extract<dict>(historyTuple[ii]);
        boost::python::list keys = (boost::python::list)dictElement.keys();

        //Because the dictionary key is put in the reversed order ,
        //we need to make a loop from the last index to keep the correct sequence.
        if(_debug)
            cout << "[Pyrfa::historySubmit] fieldList: ";

        for (int j = len(keys) - 1 ; j >= 0; j--) {
            string key = extract<string>(keys[j]);
            string value = extract<string>(str(dictElement[keys[j]]));
            if(_debug) {
                cout << key.c_str() << "=" << value.c_str();
                if (j != 0 ) {
                    cout << ",";
                }
            }
            // Exclude RIC key as not defined in RDM dictionary
            if (key == "RIC") {
                symbolName = value;
                continue;
            } else if (key == "MTYPE") {
                mtype = value;
                boost::to_lower(mtype);
                continue;
            } else if (key == "SERVICE") {
                serviceName = value.c_str();
                continue;
            }

            // construct fieldList for the given symbolName
            fieldList.push_back(rfa::common::RFA_String(key.c_str()));
            fieldList.push_back(rfa::common::RFA_String(value.c_str()));
        }
        if(_debug)
            cout << endl;

        // submit
        if(_pOMMCProvServer == NULL) {
            _pOMMCProvServer = new OMMCProvServer(_pOMMProvider, _pLoginHandler->_pLoginHandle, _vendorName, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
            _pOMMCProvServer->setDebugMode(_debug);
        }

        // submit image/update
        if(_pLoginHandler->isLoggedIn() && _isConnectionUp)
            _pOMMCProvServer->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_HISTORY, mtype, serviceName);
    }
}

void Pyrfa::closeSubmit(object const &argv1, object const &argv2=object()) {
    if(!_pOMMCProvServer && !_pOMMInteractiveProvider)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv1);
    #else
    std::string itemName = extract<string>(str(argv1).encode("utf-8"));
    #endif

    std::string sessionOrSeviceName = "";
    if (argv2 != object()) {
        #if PY_MAJOR_VERSION >= 3
        sessionOrSeviceName = extract<string>(argv2);
        #else
        sessionOrSeviceName = extract<string>(str(argv2).encode("utf-8"));
        #endif
    }

    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        if (_pOMMCProvServer) {
            if(sessionOrSeviceName == "")
                sessionOrSeviceName = _serviceName;
            _pOMMCProvServer->closeSubmit(itemList[i].c_str(), sessionOrSeviceName);
        }
        if (_pOMMInteractiveProvider) {
            _pOMMInteractiveProvider->closeSubmit(itemList[i].c_str(), sessionOrSeviceName);
        }
    }
    dispatchEventQueue(0);
}

void Pyrfa::closeAllSubmit() {
    if(_pOMMCProvServer) {
        _pOMMCProvServer->closeAllSubmit();
    }
    if(_pOMMInteractiveProvider) {
        _pOMMInteractiveProvider->closeAllSubmit();
    }
}

void Pyrfa::staleSubmit(object const &argv1, object const &argv2=object()) {
    if(!_pOMMCProvServer && !_pOMMInteractiveProvider)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string itemName = extract<string>(argv1);
    #else
    std::string itemName = extract<string>(str(argv1).encode("utf-8"));
    #endif

    std::string sessionOrSeviceName = "";
    if (argv2 != object()) {
        #if PY_MAJOR_VERSION >= 3
        sessionOrSeviceName = extract<string>(argv2);
        #else
        sessionOrSeviceName = extract<string>(str(argv2).encode("utf-8"));
        #endif
    }

    vector<string> itemList;
    boost::split(itemList, itemName, boost::is_any_of(","));
    for(size_t i = 0; i < itemList.size(); i++) {
        boost::trim(itemList[i]);
        if(itemList[i].empty())
            continue;
        if (_pOMMCProvServer) {
            if(sessionOrSeviceName == "")
                sessionOrSeviceName = _serviceName;
            _pOMMCProvServer->staleSubmit(itemList[i].c_str(), sessionOrSeviceName);
        }
        if (_pOMMInteractiveProvider) {
            _pOMMInteractiveProvider->staleSubmit(itemList[i].c_str(), sessionOrSeviceName);
        }
    }
    dispatchEventQueue(0);
}

void Pyrfa::staleAllSubmit() {
    if(_pOMMCProvServer) {
        _pOMMCProvServer->staleAllSubmit();
    }
    if(_pOMMInteractiveProvider) {
        _pOMMInteractiveProvider->staleAllSubmit();
    }
}
void Pyrfa::serviceDownSubmit(object const &argv=object()) {
    std::string servicename = "";
    if(argv != object()) {
        #if PY_MAJOR_VERSION >= 3
        servicename = extract<string>(argv);
        #else
        servicename = extract<string>(str(argv).encode("utf-8"));
        #endif
    }
    rfa::common::RFA_String serviceName(servicename.c_str());
    if(_pOMMCProvServer) {
        if(serviceName.empty())
            serviceName = _serviceName.c_str();
        _pOMMCProvServer->serviceStateSubmit(0, serviceName);
        _pOMMCProvServer->closeAllSubmit(serviceName);
    }
    if(_pOMMInteractiveProvider) {
        serviceName = _serviceName.c_str();
        _pOMMInteractiveProvider->serviceStateSubmit(0, serviceName);
    }
    dispatchEventQueue(500);
}

void Pyrfa::serviceUpSubmit(object const &argv=object()) {
    std::string servicename = "";
    if(argv != object()) {
        #if PY_MAJOR_VERSION >= 3
        servicename = extract<string>(argv);
        #else
        servicename = extract<string>(str(argv).encode("utf-8"));
        #endif
    }
    rfa::common::RFA_String serviceName(servicename.c_str());
    if(_pOMMCProvServer) {
        if(serviceName.empty())
            serviceName = _serviceName.c_str();
        _pOMMCProvServer->serviceStateSubmit(1, serviceName);
    }
    if(_pOMMInteractiveProvider) {
        serviceName = _serviceName.c_str();
        _pOMMInteractiveProvider->serviceStateSubmit(1, serviceName);
    }
    dispatchEventQueue(500);
}

void Pyrfa::logoutSubmit(object const &argv) {
    if(!_pOMMInteractiveProvider)
        return;

    #if PY_MAJOR_VERSION >= 3
    std::string session = extract<string>(argv);
    #else
    std::string session = extract<string>(str(argv).encode("utf-8"));
    #endif

    _log = "[Pyrfa::logoutSubmit] Logging out session: ";
    _log.append(session.c_str());
    _logInfo(_log.c_str());

    vector<string> sessionList;
    boost::split(sessionList, session, boost::is_any_of(","));
    for(size_t i = 0; i < sessionList.size(); i++) {
        boost::trim(sessionList[i]);
        if(sessionList[i].empty())
            continue;
        _pOMMInteractiveProvider->logoutSubmit(sessionList[i]);
    }
    dispatchEventQueue(0);
}

void Pyrfa::logoutAllSubmit() {
    if(!_pOMMInteractiveProvider)
        return;

    _pOMMInteractiveProvider->logoutAllSubmit();
    dispatchEventQueue(0);
}

/*
* OMM Posting
*/
void Pyrfa::marketPricePost(object const &argv) {
    dispatchEventQueue(0);
    if(!_pOMMConsumer) {
        _log = "[Pyrfa::marketPricePost] ERROR. Must call createOMMConsumer first.";
        _logError(_log.c_str());
        return;
    }

    std::string symbolName;
    std::string mtype;
    rfa::common::RFA_String serviceName;
    rfa::common::RFA_Vector<rfa::common::RFA_String> fieldList;

    // if argv is a dict then make it to tuple
    extract<dict> isDict(argv);
    boost::python::tuple marketpriceTuple;
    if (isDict.check()) {
        marketpriceTuple = boost::python::make_tuple(argv);
    } else {
        marketpriceTuple = extract<boost::python::tuple>(argv);
    }

    for (int ii = 0 ; ii < len(marketpriceTuple); ii++) {
        fieldList.clear();
        mtype = "update";
        symbolName = "";
        serviceName = _serviceName.c_str();

        dict dictElement = extract<dict>(marketpriceTuple[ii]);
        boost::python::list keys = (boost::python::list)dictElement.keys();

        //Because the dictionary key is put in the reversed order ,
        //we need to make a loop from the last index to keep the correct sequence.
        if(_debug)
            cout << "[Pyrfa::marketPricePost] fieldList: ";

        for (int j = len(keys) - 1 ; j >= 0; j--) {
            string key = extract<string>(keys[j]);
            string value = extract<string>(str(dictElement[keys[j]]));
            if(_debug) {
                cout << key.c_str() << "=" << value.c_str();
                if (j != 0 ) {
                    cout << ",";
                }
            }
            // Exclude RIC key as not defined in RDM dictionary
            if (key == "RIC") {
                symbolName = value;
                continue;
            } else if (key == "MTYPE") {
                mtype = value;
                boost::to_lower(mtype);
                continue;
            } else if (key == "SERVICE") {
                serviceName = value.c_str();
                continue;
            }
            // construct fieldList for the given symbolName
            fieldList.push_back(rfa::common::RFA_String(key.c_str()));
            fieldList.push_back(rfa::common::RFA_String(value.c_str()));
        }
        if(_debug)
            cout << endl;

        // submit
        if(_pOMMPost == NULL) {
            _pOMMPost = new OMMPost(_pOMMConsumer, _pLoginHandler->_pLoginHandle, _pDictionaryHandler->getDictionary(), *_pComponentLogger);
            _pOMMPost->setDebugMode(_debug);
        }

        // submit image/update
        if(_pLoginHandler->isLoggedIn() && _isConnectionUp)
            _pOMMPost->submitData(symbolName.c_str(), fieldList, rfa::rdm::MMT_MARKET_PRICE, mtype, serviceName);
    }
}


/*
* Optimized Pasue and Resume (all items/all domains)
*/
void Pyrfa::pauseAll() {
    if(!_pLoginHandler)
        return;

    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        _pLoginHandler->pauseAll(_userName);
    } else {
        _logError("[Pyrfa::pauseAll] Dictionary not made available or user not logged in");
    }
    if(_debug) {
        _log = "[Pyrfa::pauseAll] Pause all subscription.";
        _logInfo(_log.c_str());
    }
}

void Pyrfa::resumeAll() {
    if(!_pLoginHandler)
        return;

    if (_pDictionaryHandler->isAvailable() && _pLoginHandler->isLoggedIn()) {
        _pLoginHandler->resumeAll(_userName);
    } else {
        _logError("[Pyrfa::resumeAll] Dictionary not made available or user not logged in");
    }
    if(_debug) {
        _log = "[Pyrfa::resumeAll] Resume all subscription.";
        _logInfo(_log.c_str());
    }
}

void Pyrfa::processOMMItemEvent(const rfa::sessionLayer::OMMItemEvent &OMMEvent)
{
    // output string meant for Python tuple
    _eventData = boost::python::tuple();

    const rfa::message::RespMsg& respMsg = static_cast<const rfa::message::RespMsg&> (OMMEvent.getMsg());
    switch (respMsg.getMsgModelType())
    {
    case rfa::rdm::MMT_LOGIN:
        _pLoginHandler->processResponse(respMsg);
        break;
    case rfa::rdm::MMT_DIRECTORY:
        _pDirectoryHandler->processResponse(respMsg, _eventData);
        break;
    case rfa::rdm::MMT_DICTIONARY:
        _pDictionaryHandler->processResponse(respMsg);
        break;
    case rfa::rdm::MMT_SYMBOL_LIST:
        _pSymbolListHandler->processResponse(respMsg, OMMEvent.getHandle(), _eventData);
        break;
    case rfa::rdm::MMT_MARKET_PRICE:
        if(_pMarketPriceHandler) {
            _pMarketPriceHandler->processResponse(respMsg, OMMEvent.getHandle(), _eventData);
        } else if(_pTimeSeriesHandler) {
            _pTimeSeriesHandler->processResponse(respMsg, OMMEvent.getHandle(), _eventData);
        }
        break;
    case rfa::rdm::MMT_MARKET_BY_ORDER:
        _pMarketByOrderHandler->processResponse(respMsg, OMMEvent.getHandle(), _eventData);
        break;
    case rfa::rdm::MMT_MARKET_BY_PRICE:
        _pMarketByPriceHandler->processResponse(respMsg, OMMEvent.getHandle(), _eventData);
        break;
    case rfa::rdm::MMT_HISTORY:
        _pHistoryHandler->processResponse(respMsg, OMMEvent.getHandle(), _eventData);
        break;
    default:
        _log = "[Pyrfa::processOMMItemEvent] Message Model <";
        _log.append((int) respMsg.getMsgModelType());
        _log += "> not handled!! ";
        _logError(_log.c_str());
        break;
    }
}

void Pyrfa::processOMMCmdErrorEvent(const rfa::sessionLayer::OMMCmdErrorEvent &CmdEvent) {
    _log = "[Pyrfa::processOMMCmdErrorEvent] Command error!!!!!!!!!!!!";
    _logError(_log.c_str());
}

void Pyrfa::setInteractionType(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    std::string interactionType = extract<string>(argv);
    #else
    std::string interactionType = extract<string>(str(argv).encode("utf-8"));
    #endif
    if(interactionType == "snapshot") {
        _interactionType = rfa::message::ReqMsg::InitialImageFlag;
    } else {
        _interactionType = (rfa::message::ReqMsg::InitialImageFlag | rfa::message::ReqMsg::InterestAfterRefreshFlag);
    }
}

void Pyrfa::setView(object const &argv=object()) {
    std::string viewFIDs = "";
    if(argv != object()) {
        #if PY_MAJOR_VERSION >= 3
        viewFIDs = extract<string>(argv);
        #else
        viewFIDs = extract<string>(str(argv).encode("utf-8"));
        #endif
    }
    _viewFIDs = viewFIDs;
}

int Pyrfa::getFieldType(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* fieldName = extract<const char*>(argv);
    #else
    const char* fieldName = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    if(!_pDictionaryHandler)
        return 0;
    const RDMFieldDef* fieldDef = NULL;
    fieldDef = _pDictionaryHandler->getDictionary()->getFieldDef(fieldName);
    if(!fieldDef) {
        return (int)rfa::data::DataBuffer::UnknownDataBufferEnum;
    }
    return (int)fieldDef->getDataType();
}

int Pyrfa::getFieldID(object const &argv) {
    #if PY_MAJOR_VERSION >= 3
    const char* fieldName = extract<const char*>(argv);
    #else
    const char* fieldName = extract<const char*>(str(argv).encode("utf-8"));
    #endif
    if(!_pDictionaryHandler)
        return 0;
    const RDMFieldDef* fieldDef = NULL;
    fieldDef = _pDictionaryHandler->getDictionary()->getFieldDef(fieldName);
    if(!fieldDef) {
        return 0;
    }
    return (int)fieldDef->getFieldId();
}

std::string Pyrfa::getClientSessions() {
    if(!_pOMMInteractiveProvider)
        return "";

    std::string sessions = _pOMMInteractiveProvider->getClientSessions();
    boost::trim(sessions);
    return sessions;
}

std::string Pyrfa::getClientWatchList(object const &argv) {
    if(!_pOMMInteractiveProvider)
        return "";

    #if PY_MAJOR_VERSION >= 3
    std::string session = extract<string>(argv);
    #else
    std::string session = extract<string>(str(argv).encode("utf-8"));
    #endif

    std::string watchlist = _pOMMInteractiveProvider->getClientWatchList(session);
    boost::trim(watchlist);
    return watchlist;
}

/*
* Exception translator for Python. See py_error class in pyrfa.h.
*/
static void translateException(const py_error &err) {
    PyErr_SetString(PyExc_Exception, err.what().c_str());
}

/*
* Overloading some functions
*/
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_printConfigDb_overloads, Pyrfa::printConfigDb, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_setDebugMode_overloads, Pyrfa::setDebugMode, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_login_overloads, Pyrfa::login, 0, 4)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_dispatchEventQueue_overloads, Pyrfa::dispatchEventQueue, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_directorySubmit_overloads, Pyrfa::directorySubmit, 0, 2)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_symbolListRequest_overloads, Pyrfa::symbolListRequest, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_serviceUpSubmit_overloads, Pyrfa::serviceUpSubmit, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_serviceDownSubmit_overloads, Pyrfa::serviceDownSubmit, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_setView_overloads, Pyrfa::setView, 0, 1)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_closeSubmit_overloads, Pyrfa::closeSubmit, 1, 2)
BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(Pyrfa_staleSubmit_overloads, Pyrfa::staleSubmit, 1, 2)

BOOST_PYTHON_MODULE(pyrfa)
{
    register_exception_translator<py_error>(&translateException);
    class_<Pyrfa>("Pyrfa")
        .def("log", &Pyrfa::logInfo)
        .def("logInfo", &Pyrfa::logInfo)
        .def("logWarning", &Pyrfa::logWarning)
        .def("logError", &Pyrfa::logError)
        .def("setDebugMode", &Pyrfa::setDebugMode, Pyrfa_setDebugMode_overloads( args("argv") ))
        //.def("initializeRFA", &Pyrfa::initializeRFA)
        //.def("uninitializeRFA", &Pyrfa::uninitializeRFA)
        .def("createConfigDb", &Pyrfa::createConfigDb)
        .def("printConfigDb", &Pyrfa::printConfigDb, Pyrfa_printConfigDb_overloads( args("argv") ))
        .def("getConfigDb", &Pyrfa::getConfigDb)
        .def("createEventQueue", &Pyrfa::createEventQueue)
        .def("acquireLogger", &Pyrfa::acquireLogger)
        .def("acquireSession", &Pyrfa::acquireSession)
        .def("createOMMConsumer", &Pyrfa::createOMMConsumer)
        .def("createOMMProvider", &Pyrfa::createOMMProvider)
        .def("login", &Pyrfa::login, Pyrfa_login_overloads( args("argv1","argv2","argv3","argv4") ))
        .def("isLoggedIn",&Pyrfa::isLoggedIn)
        .def("directoryRequest", &Pyrfa::directoryRequest)
        .def("dictionaryRequest", &Pyrfa::dictionaryRequest)
        .def("getFieldType", &Pyrfa::getFieldType)
        .def("getFieldID", &Pyrfa::getFieldID)
        .def("isNetworkDictionaryAvailable", &Pyrfa::isNetworkDictionaryAvailable)
        .def("isConnectionUp", &Pyrfa::isConnectionUp)
        .def("symbolListRequest", &Pyrfa::symbolListRequest, Pyrfa_symbolListRequest_overloads( args("argv") ))
        .def("symbolListCloseRequest", &Pyrfa::symbolListCloseRequest)
        .def("symbolListCloseAllRequest", &Pyrfa::symbolListCloseAllRequest)
        .def("isSymbolListRefreshComplete", &Pyrfa::isSymbolListRefreshComplete)
        .def("isHistoryRefreshComplete", &Pyrfa::isHistoryRefreshComplete)
        .def("setServiceName", &Pyrfa::setServiceName)
        .def("getServiceName", &Pyrfa::getServiceName)
        .def("getSymbolList", &Pyrfa::getSymbolList)
        .def("getTimeSeries", &Pyrfa::getTimeSeries)
        .def("getWatchList", &Pyrfa::getMarketPriceWatchList)
        .def("getSymbolListWatchList", &Pyrfa::getSymbolListWatchList)
        .def("getMarketPriceWatchList", &Pyrfa::getMarketPriceWatchList)
        .def("getMarketByOrderWatchList", &Pyrfa::getMarketByOrderWatchList)
        .def("getMarketByPriceWatchList", &Pyrfa::getMarketByPriceWatchList)
        .def("getTimeSeriesWatchList", &Pyrfa::getTimeSeriesWatchList)
        .def("getHistoryWatchList", &Pyrfa::getHistoryWatchList)
        .def("marketPriceRequest", &Pyrfa::marketPriceRequest)
        .def("marketPriceCloseRequest", &Pyrfa::marketPriceCloseRequest)
        .def("marketPriceCloseAllRequest", &Pyrfa::marketPriceCloseAllRequest)
        .def("marketPricePause", &Pyrfa::marketPricePause)
        .def("marketPriceResume", &Pyrfa::marketPriceResume)
        .def("marketByOrderRequest", &Pyrfa::marketByOrderRequest)
        .def("marketByOrderCloseRequest", &Pyrfa::marketByOrderCloseRequest)
        .def("marketByOrderCloseAllRequest", &Pyrfa::marketByOrderCloseAllRequest)
        .def("marketByPriceRequest", &Pyrfa::marketByPriceRequest)
        .def("marketByPriceCloseRequest", &Pyrfa::marketByPriceCloseRequest)
        .def("marketByPriceCloseAllRequest", &Pyrfa::marketByPriceCloseAllRequest)
        .def("setInteractionType", &Pyrfa::setInteractionType)
        .def("setView", &Pyrfa::setView, Pyrfa_setView_overloads( args("argv") ))
        .def("setTimeSeriesPeriod", &Pyrfa::setTimeSeriesPeriod)
        .def("setTimeSeriesMaxRecords", &Pyrfa::setTimeSeriesMaxRecords)
        .def("timeSeriesRequest", &Pyrfa::timeSeriesRequest)
        .def("timeSeriesCloseRequest", &Pyrfa::timeSeriesCloseRequest)
        .def("timeSeriesCloseAllRequest", &Pyrfa::timeSeriesCloseAllRequest)
        .def("historyRequest", &Pyrfa::historyRequest)
        .def("historyCloseRequest", &Pyrfa::historyCloseRequest)
        .def("historyCloseAllRequest", &Pyrfa::historyCloseAllRequest)
        .def("directorySubmit", &Pyrfa::directorySubmit, Pyrfa_directorySubmit_overloads( args("argv1","argv2") ))
        .def("marketPriceSubmit", &Pyrfa::marketPriceSubmit)
        .def("marketByOrderSubmit", &Pyrfa::marketByOrderSubmit)
        .def("marketByPriceSubmit", &Pyrfa::marketByPriceSubmit)
        .def("symbolListSubmit", &Pyrfa::symbolListSubmit)
        .def("historySubmit", &Pyrfa::historySubmit)
        .def("closeSubmit", &Pyrfa::closeSubmit, Pyrfa_closeSubmit_overloads( args("argv2") ))
        .def("closeAllSubmit", &Pyrfa::closeAllSubmit)
        .def("staleSubmit", &Pyrfa::staleSubmit, Pyrfa_staleSubmit_overloads( args("argv2") ))
        .def("staleAllSubmit", &Pyrfa::staleAllSubmit)
        .def("serviceUpSubmit", &Pyrfa::serviceUpSubmit, Pyrfa_serviceUpSubmit_overloads( args("argv") ))
        .def("serviceDownSubmit", &Pyrfa::serviceDownSubmit, Pyrfa_serviceDownSubmit_overloads( args("argv") ))
        .def("logoutSubmit", &Pyrfa::logoutSubmit)
        .def("logoutAllSubmit", &Pyrfa::logoutAllSubmit)
        .def("marketPricePost", &Pyrfa::marketPricePost)
        .def("pauseAll", &Pyrfa::pauseAll)
        .def("resumeAll", &Pyrfa::resumeAll)
        .def("getClientSessions", &Pyrfa::getClientSessions)
        .def("getClientWatchList", &Pyrfa::getClientWatchList)
        .def("dispatchEventQueue", &Pyrfa::dispatchEventQueue, Pyrfa_dispatchEventQueue_overloads( args("timeout") ))
        //.def("deactivateEventQueue", &Pyrfa::deactivateEventQueue);
        ;
}
