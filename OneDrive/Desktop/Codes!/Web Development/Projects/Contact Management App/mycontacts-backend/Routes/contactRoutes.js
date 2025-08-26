const express = require("express");
const router = express.Router();
const {
  getContact,
  createContact,
  getContacts,
  updateContact,
  deleteContact,
} = require("../controllers/contactController");
const validateToken = require("../middleware/validateTokenHandler");

// Apply the validation middleware
router.use(validateToken);

// Define routes
router.route("/").get(getContacts).post(createContact);
router.route("/:id").get(getContact).put(updateContact).delete(deleteContact);

module.exports = router;
