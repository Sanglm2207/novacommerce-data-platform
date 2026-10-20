Seed the training database after applying the schema migration:

```sh
python database/seeds/seed.py
```

The script requires the five seed tables to be empty. If they already contain
data and you want to replace it, run:

```sh
python database/seeds/seed.py --reset
```

`--reset` deletes all rows in `customers`, `products`, `orders`, `order_items`,
and `payments`, resets their identity sequences, and generates a fresh dataset.
The reset and inserts run in one transaction, so a failure restores the previous
data. Only the `novacommerce_oltp` training database is accepted. Connection
credentials are loaded from the root `.env` file.
