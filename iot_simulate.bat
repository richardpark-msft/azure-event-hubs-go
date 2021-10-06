rem Send mixed properties (http)

rem incorrect
rem iothub-message-id=myiothubmessageid
rem user-id=arbitraryuserid

rem correct
rem iothub-messageid=myiothubmessageidnodash
rem iothub-correlationid=myiothubcorrelation-id
rem this seems to get hashed or translated (it's consistent, however)
rem iothub-userid=arbitraryuseridnodash

rem From the CLI help:
rem For http messaging - application properties are sent using iothub-app-<name>=value, for instance iothub-app- myprop=myvalue. System properties are generally prefixed with iothub-<name> like iothub-correlationid but there are exceptions such as content-type and content-encoding.

az iot device simulate -n riparkdev -d testdevice1 --protocol http ^
  --properties "iothub-app-myprop=myvalue;iothub-messageid=myiothubmessageidnodash;iothub-correlationid=myiothubcorrelation-id;iothub-dt-subject=mydtsubject;iothub-subject=mysubject;iothub-dtsubject=mydtsubjectnodash;content-type=hellocontenttypewithdash;contenttype=hellocontenttype; iothub-user-id=arbitraryuseridwithdash;iothub-userid=arbitraryuseridnodash;iothub-app-someproperty=somevalue"