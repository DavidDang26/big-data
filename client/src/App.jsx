import { useState } from "react";
import reactLogo from "./assets/react.svg";
import viteLogo from "/vite.svg";
import {
  getByBusiness,
  getByGaming,
  getByNormal,
  getByOffice,
} from "./data/laptops";
import "./App.css";

function App() {
  const [category, setCategory] = useState("");
  const [laptops, setLaptops] = useState([]);

  const searchLaptop = async () => {
    let data;
    switch (category) {
      case "normal":
        data = await getByNormal();
        break;

      case "gaming":
        data = await getByGaming();
        break;

      case "business":
        data = await getByBusiness();
        break;

      case "office":
        data = await getByOffice();
        break;

      default:
        break;
    }
    console.log("🚀 ~ file: App.jsx:13 ~ searchLaptop ~ data:", data);
    setLaptops(Object.values(data).slice(0, 10));
  };

  return (
    <>
      <div>What do you use your laptop for?</div>
      <input
        className="input-category"
        type="text"
        value={category}
        onChange={(e) => setCategory(e.target.value)}
      />
      <button onClick={searchLaptop}>Search</button>
      <div>Here is the recommend laptop:</div>
      {laptops.map((laptop) => {
        if (!laptop) return <div></div>;
        return (
          <div key={laptop.name} className="laptop-container">
            <img className="laptop-img" src={laptop.image} alt="" />
            <div className="laptop-statistic">
              <div>Name: {laptop.name}</div>
              <div>CPU: {laptop.CPU}</div>
              <div>Card: {laptop.Card}</div>
              <div>Ram: {laptop.RAM}</div>
              <div>
                Price: {new Intl.NumberFormat().format(laptop.currentPrice)}đ
              </div>
              <div>
                Analytics Score:{" "}
                {laptop.score ||
                  laptop.office_score ||
                  laptop.gaming_score ||
                  laptop.business_score}
              </div>
            </div>
          </div>
        );
      })}
    </>
  );
}

export default App;
