const {constants} = require("../constants")
const errorHandler = (err,req,res,next) => {
 const statusCode = res.statusCode ? res.statusCode : 500;

switch (statusCode) {
  case constants.VALIDATION_ERROR:
   res.json({ titile: "Validation Failed", message: err.messsage, stackTrace: err.stack});
  case constants.NOT_FOUND:
   res.json({ titile: "Not Found", message: err.messsage, stackTrace: err.stack});
  case constants.UNAUTHORIZED:
   res.json({ titile: "Unauthorized", message: err.messsage, stackTrace: err.stack});
  case constants.FORBIDDEN:
   res.json({ titile: "Forbidden", message: err.messsage, stackTrace: err.stack});
  case constants.SERVER_ERROR:
   res.json({ titile: "Server Error", message: err.messsage, stackTrace: err.stack});

  default:
  console.log("No Error, All good!");
  break;

}

 res.json({ titile: "Not Found", message: err.messsage, stackTrace: err.stack});
 res.json({ titile: "Not Found", message: err.messsage, stackTrace: err.stack});

}

module.exports = errorHandler