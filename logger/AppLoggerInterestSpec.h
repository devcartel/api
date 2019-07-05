#ifndef __rfa_AppLoggerInterestSpec_h
#define __rfa_AppLoggerInterestSpec_h

/**
    \class rfa::logger::AppLoggerInterestSpec AppLoggerInterestSpec.h "Logger/AppLoggerInterestSpec.h"
 
    \brief 
    AppLoggerInterestSpec  provides message filter ability used to define 
    an interest in set of messages generated by logger component.

    AppLoggerInterestSpec  provides message filter ability used to define 
    an interest in set of messages generated by logger component.

    @see    AppLoggerMonitor,
             rfa::common::InterestSpec
*/

#include "Logger/Logger.h"
#include "Common/InterestSpec.h"
#include "Logger/LoggerEnums.h"

// \namespace rfa
namespace rfa {

// \namespace logger
namespace logger{

class AppLoggerInterestSpecImpl;

class RFA_LOGGER_API AppLoggerInterestSpec : public  rfa::common::InterestSpec  
{
        
public:
///@name Constructor
//@{
/** Default Constructor */
    AppLoggerInterestSpec();

/** Copy Constructor */
	AppLoggerInterestSpec( const AppLoggerInterestSpec & interestSpec);
//@}

///@name Destructor
//@{
/** Destructor */
    virtual ~AppLoggerInterestSpec();
//@}

    


///@name Operations
//@{

	/** Assignment operator */
	AppLoggerInterestSpec & operator= ( const AppLoggerInterestSpec & interstSpec);


    /** Set the minimum servirity of the messages  to listen to. */
    void  setMinSeverity(const long minSeverity);
   
	
	/** Create a clone of this interest specification
		\return A pointer to a copy of the original InterestSpec
	*/
	virtual rfa::common::InterestSpec* clone() const;

//@}

///@name Attributes
//@{
    /** Get the minimum log message serverity  */
    long  getMinSeverity() const;
//@}




private:

	AppLoggerInterestSpecImpl* _pImpl;

};//end of class AppLoggerInterestSpec

}// \namespace logger

}// \namespace rfa
#endif 
