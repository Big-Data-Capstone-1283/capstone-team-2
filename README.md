# Big Data Capstone Project
## Table of Contents

* <a href="#project-description">Project Description</a>
* <a href="#team-members">Team Members</a>
* <a href="#technologies">Technologies</a>
* <a href="#schema">Schema</a>

## Project Description
In this project each team is tasked with creating random e-commerce transaction data according to the below schema, and publishing it to a topic stored on a shared Kafka Broker. After each team publishes their data to their respective topics the other team consumes the data from the other team's topic. This means in our case we're publishing data to the "team2" topic and consuming data from the "team1" topic. After exchanging and cleaning the data we're performing analysis on the data via our pattern detection classes and exporting the data to create visualizations. 

## Team Members
- Adam Gore [@Adam-Gore96](https://github.com/Adam-Gore96)
- Alex White [@AlexWhite252](https://github.com/AlexWhite252)
- Brandon Cho [@BrandonYCho](https://github.com/BrandonYCho)
- Brian Vegh [@brianvegh](https://github.com/brianvegh)
- Douglas Lam [@Douglas-Lam](https://github.com/Douglas-Lam)
- Evan Laferriere [@evanlaferriere](https://github.com/evanlaferriere)
- Jeffrey Hafner [@JeffH001](https://github.com/JeffH001)
- Md Tahmid Khan [@MdTahmidKhan](https://github.com/MdTahmidKhan)
- Patrick Froerer [@PJFroerer](https://github.com/PJFroerer)
- Rudy Esquerra [@rudyesquerra](https://github.com/rudyesquerra)

## Technologies
- Scala 2.13.8
- Apache Spark 3.2.0
- Apache Kafka 3.1.0

## Schema

| Field name             | Description                         |
|------------------------|-------------------------------------|
| order_id               | Order Id                            |
| customer_id            | Customer Id                         |
| customer_name          | Customer Name                       |
| product_id             | Product Id                          |
| product_name           | Product Name                        |
| product_cateogry       | Product Category                    |
| payment_type           | Payment Type                        |
| qty                    | Quantity ordered                    |
| price                  | Price of the product                |
| datetime               | Date and time when order was placed |
| country                | Customer Country                    |
| city                   | Customer City                       |
| ecommerce_website_name | Site from where order was placed    |
| payment_txn_id         | Payment Transaction Confirmation Id |
| payment_txn_success    | Payment Success or Failure          |
| failure_reason         | Reason for payment failure          |
