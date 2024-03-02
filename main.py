import json
import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Book, Stock, Shop, Sale

DSN = "postgresql://postgres:postgres@localhost:5432/orm_db"
engine = sq.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open("test_data.json", "r") as bd:
    data = json.load(bd)

for record in data:
    model = {
        "publisher": Publisher,
        "shop": Shop,
        "book": Book,
        "stock": Stock,
        "sale": Sale,
    }[record.get("model")]
    session.add(model(id=record.get("pk"), **record.get("fields")))
session.commit()


def get_shops(search=input("Введите идентификатор или имя автора: ")):
    search = search
    if search.isnumeric():
        results = (
            session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)
            .join(Publisher, Publisher.id == Book.id_publisher)
            .join(Stock, Stock.id_book == Book.id)
            .join(Shop, Shop.id == Stock.id_shop)
            .join(Sale, Sale.id_stock == Stock.id)
            .filter(Publisher.id == search)
            .all()
        )
        for book, shop, price, date in results:
            print(f"{book: <40} | {shop: <10} | {price: <10} | {date}")
    else:
        results = (
            session.query(Book.title, Shop.name, Sale.price, Sale.date_sale)
            .join(Publisher, Publisher.id == Book.id_publisher)
            .join(Stock, Stock.id_book == Book.id)
            .join(Shop, Shop.id == Stock.id_shop)
            .join(Sale, Sale.id_stock == Stock.id)
            .filter(Publisher.name == search)
            .all()
        )
        for book, shop, price, date in results:
            print(f"{book: <40} | {shop: <10} | {price: <10} | {date}")


session.close()


if __name__ == "__main__":
    get_shops()
