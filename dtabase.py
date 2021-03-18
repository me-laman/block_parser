import datetime

from gino import Gino

import sqlalchemy as sa
from pycoin.symbols.tbtx import network

from utils import get_address_name


db = Gino()


class Address(db.Model):
    __tablename__ = "addresses"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    address = db.Column(db.String(100), nullable=False, unique=True, index=True)

    def __repr__(self):
        return self.address


class Transaction(db.Model):
    __tablename__ = "transactions"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    tx_hash = db.Column(db.String(64), nullable=False, index=True, unique=True)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    block_number = db.Column(db.Integer, nullable=True, index=True)
    position = db.Column(db.Integer, nullable=True)
    tx_dict = db.Column(db.JSON, nullable=True)


class ToAddress(db.Model):
    __tablename__ = "to_addresses"  # tx_outs

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    transaction_id = db.Column(db.Integer, db.ForeignKey('transactions.id'), nullable=True)

    address_id = db.Column(db.Integer, db.ForeignKey('addresses.id'), nullable=True)
    amount = db.Column(db.BIGINT, nullable=False)
    position = db.Column(db.BIGINT, nullable=False, index=True)
    script = db.Column(db.Text, nullable=False)


class FromAddress(db.Model):
    __tablename__ = "from_addresses"  # tx_ins

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    transaction_id = db.Column(db.Integer, db.ForeignKey('transactions.id'), nullable=False)
    address_id = db.Column(db.Integer, db.ForeignKey('addresses.id'), nullable=True)
    previous_id = db.Column(db.Integer, db.ForeignKey('to_addresses.id'))  # child


async def get_previous(hash, position):
    query = ToAddress.join(Transaction).select()
    to_address = await query \
        .where(sa.and_(Transaction.tx_hash == hash, ToAddress.position == position)) \
        .gino \
        .first()

    return to_address


async def get_or_create_address(address):
    if (address_obj := await Address.query.where(Address.address == address).gino.first()) is not None:
        return address_obj

    address_obj = Address(address=address)
    return await address_obj.create()


async def create_transaction(block_number, ix, blockchain_tx):
    tx = Transaction(
        tx_hash=blockchain_tx.id(),
        position=ix,
        block_number=block_number,
    )
    transaction = await tx.create()

    for ix, txs_out in enumerate(blockchain_tx.txs_out):
        address_hash = get_address_name(txs_out, network=network)
        address = await get_or_create_address(address=address_hash)

        to_address = ToAddress(
            address_id=address.id,
            amount=txs_out.coin_value,
            script=txs_out.script.hex(),
            position=ix,
            transaction_id=transaction.id,
        )
        to_address = await to_address.create()
        transaction.add_to_address = to_address

    for txs_in in blockchain_tx.txs_in:
        prev = await get_previous(str(txs_in.previous_hash), txs_in.previous_index)
        address_hash = get_address_name(txs_in, network=network)
        address = await get_or_create_address(address=address_hash)

        from_address = FromAddress(
            address_id=address.id,
            transaction_id=transaction.id,
            previous_id=prev[0] if prev else None
        )
        await from_address.create()

    return transaction