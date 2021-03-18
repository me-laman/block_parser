import io
import asyncio

from bitcoinrpc import BitcoinRPC
from pycoin.encoding.hexbytes import h2b
from pycoin.symbols.tbtx import network

from elasticsearch import AsyncElasticsearch

from dtabase import db, Transaction, create_transaction
from utils import serialize_transaction

Block = network.block

rpc = BitcoinRPC("127.0.0.1", 18332, "bc_test", "Emh67ztmLs2r5tdRUF9b")


es = AsyncElasticsearch()


async def main():
    await db.set_bind('postgres://chain:chain@localhost:5432/blockchain')
    await db.gino.create_all()

    block_number = 0
    while (block_number := block_number + 1) < await rpc.getblockcount():
        block = await rpc.getblockhash(block_number)
        block_hash = await rpc.getblock(block, verbosity=0)
        block_data = Block.parse(io.BytesIO(h2b(block_hash)), include_transactions=1)

        for ix, tx in enumerate(block_data.txs):
            if await Transaction.query.where(Transaction.tx_hash == tx.id()).gino.first():
                continue

            transaction = await create_transaction(block_number, ix, tx)

            tx_json = serialize_transaction(tx)
            await transaction.update(tx_dict=tx_json).apply()
            await es.index(index='tx', doc_type='transactions', body=tx_json)

    await db.pop_bind().close()
    await es.close()


if __name__ == "__main__":
    asyncio.run(main())