const express = require("express");
const dotenv = require("dotenv").config();
const port = process.env.PORT || 5000;
const routes = require("./Routes/contactRoutes");
const userRoutes = require("./Routes/userRoutes")
const asyncHandler = require("express-async-handler");
const errorHandler = require("./middleware/errorHandler");
const connectDb = require("./Config/dbConnection");
connectDb();
const app = express();

app.use(express.json());
app.use('/api/contacts', routes);
app.use('/api/users',userRoutes)
app.use(asyncHandler)
app.use(errorHandler);
app.listen(port, ()=> {
    console.log(`Serve running on port ${port}`)
})