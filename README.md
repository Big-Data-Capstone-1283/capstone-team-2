# Big Data Capstone Project
## Table of Contents

* <a href="#project-description">Project Description</a>
* <a href="#team-members">Team Members</a>
* <a href="#technologies">Technologies</a>
* <a href="#schema">Schema</a>

## Project Description
Big Data capstone project utilizing Kafka and Spark Streaming.

## Team Members
- Adam Gore
- Alex White [@AlexWhite252](https://github.com/AlexWhite252)
- Brandon Cho
- Brian Vegh [@brianvegh](https://github.com/brianvegh)
- Douglas Lam
- Evan Laferriere
- Jeffrey Hafner [@JeffH001](https://github.com/JeffH001)
- Md Tahmid Khan [@MdTahmidKhan](https://github.com/MdTahmidKhan)
- Patrick Froerer
- Rudy Esquerra

## Technologies
- Scala 2.13.8
- Spark 3.2.0
- Kafka 3.1.0

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
