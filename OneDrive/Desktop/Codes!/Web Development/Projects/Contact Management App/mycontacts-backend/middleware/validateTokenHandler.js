const asyncHandler = require("express-async-handler");
const jwt = require("jsonwebtoken");

const validateToken = asyncHandler(async (req, res, next) => {
  let token;
  let authHeader = req.headers.Authorization || req.headers.authorization;

  if (authHeader && authHeader.startsWith("Bearer")) {
    // Split the token from the "Bearer " prefix
    token = authHeader.split(" ")[1];

    // Verify the token
    jwt.verify(token, process.env.ACCESS_TOKEN_SECRET, (err, decoded) => {
      if (err) {
        res.status(401);
        throw new Error("User is not authorized");
      }
      // Attach the decoded user information to the request object
      req.user = decoded.user;
      next();
    });
  } else {
    res.status(401);
    throw new Error("Token is missing or not valid");
  }
});

module.exports = validateToken;
