import hashlib

# from bitcoin.core import CTxOut
from pycoin.coins.Tx import Tx
from pycoin.coins.bitcoin.Spendable import Spendable
from pycoin.coins.bitcoin.TxIn import TxIn
from pycoin.coins.bitcoin.TxOut import TxOut
from pycoin.encoding.hash import hash160
from pycoin.symbols.tbtx import network

import bech32

sha256 = lambda x: hashlib.sha256(x).digest()


def get_address_name(inorout, network):

    try:
        if type(inorout) == TxOut:
            address = network.address.for_script(inorout.puzzle_script())
        elif type(inorout) == Spendable:
            address = network.address.for_script(inorout.puzzle_script())
        else:
            # address from tx input
            if (len(inorout.witness) == 2) and (inorout.script == b''):
                # Witness PubKeyHash (pay-to-witness-pubkeyhash / P2WPKH)
                address = bech32.encode(network.ui._bech32_hrp, 0x00, hash160(inorout.witness[1]))
            elif (len(inorout.witness) > 2) and (inorout.script == b''):
                # Witness ScriptHash (pay-to-witness-scripthash / P2WSH)
                address = bech32.encode(network.ui._bech32_hrp, 0x00, sha256(inorout.witness[-1]))
            else:
                address = inorout.address(network.ui)
            if address == '(unknown)':
                address = network.ui.address_for_p2sh(hash160(inorout.script[1:]))
    except:
        address = 'bad script'
    return address


def serialize_transaction(obj):

    if hasattr(obj, 'block'):
        del obj.block
    if isinstance(obj, Tx):
        obj.tx_hash = obj.id()

    if isinstance(obj, TxOut) or isinstance(obj, TxIn):
        obj.address = get_address_name(obj, network)

    if isinstance(obj, dict):
        return {k: serialize_transaction(v) for k, v in obj.items()}
    elif isinstance(obj, bytes):
        return obj.hex()
    elif not isinstance(obj, str) and hasattr(obj, '__iter__'):
        return [serialize_transaction(v) for v in obj]
    elif hasattr(obj, '__dict__'):
        return {
            k: serialize_transaction(v)
            for k, v in obj.__dict__.items()
            if not callable(v) and not k.startswith('_')
        }
    else:
        return obj