const asyncHandler = require("express-async-handler");
const Contact = require("../Models/contactModel");

const getContact = asyncHandler(async (req, res) => {
  const contacts = await Contact.find();
  res.status(200).json(contacts);
});

const createContact = asyncHandler(async (req, res) => {
  console.log(req.body);
  const { name, email, phone } = req.body;
  if (!name || !email || !phone) {
    res.status(400);
    throw new Error("oops! not enough info");
  }
  const createContact = await Contact.create({
    name,
    email,
    phone,
    user_id: req.user.id,
  });
  res.status(201).json(createContact);
});
const getContacts = asyncHandler(async (req, res) => {
  // Check if req.user is properly populated
  if (!req.user || !req.user.id) {
    res.status(401).json({ message: "User not authenticated" });
    return;
  }

  // Query contacts by user_id
  const contacts = await Contact.find({ user_id: req.user.id });

  if (!contacts.length) {
    res.status(404).json({ message: "No contacts found for this user" });
    return;
  }

  res.status(200).json(contacts);
});

const updateContact = asyncHandler(async (req, res) => {
  const updateContact = await Contact.findByIdAndUpdate(
    req.params.id,
    req.body,
    { new: true }
  );
  if (!updateContact) {
    res.status(404);
    throw new Error("Contact Not Found!");
  }
  if (contactModel.user_id.toString() !== req.user.id) {
    res.status(403);
    throw new Error(
      "User don't have permission to update other user contacts!"
    );
  }
  res.status(300).json(updateContact);
});

const deleteContact = asyncHandler(async (req, res) => {
  const deleteContact = await Contact.findByIdAndDelete(
    req.params.id,
    req.body,
    { new: true }
  );
  res.status(300).json(deleteContact);
});

module.exports = {
  getContact,
  createContact,
  getContacts,
  updateContact,
  deleteContact,
};
