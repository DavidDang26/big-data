import { db } from "./firebase";

const laptops = () => db.ref("laptops");

export const getByBusiness = async () => {
  const ref = laptops().child("business");
  const businessLaptop = await ref.once("value");
  return businessLaptop.val();
};

export const getByGaming = async () => {
  const ref = laptops().child("gaming");
  const gamingLaptop = await ref.once("value");
  return gamingLaptop.val();
};

export const getByOffice = async () => {
  const ref = laptops().child("office");
  const officeLaptop = await ref.once("value");
  return officeLaptop.val();
};

export const getByNormal = async () => {
  const ref = laptops().child("normal");
  const normalLaptop = await ref.once("value");
  return normalLaptop.val();
};
