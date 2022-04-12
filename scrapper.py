from web3 import Web3
from web3.middleware import geth_poa_middleware
import pandas as pd
import os
import json
import utils


class OnChainScrapper:
    def __init__(
        self,
        eth_rpc_url: str,
        xdai_rpc_url: str,
        polygon_rpc_url: str,
        poap_contract_address: str,
        db_credentials: object,
    ):

        self.eth_rpc_url = eth_rpc_url
        self.xdai_rpc_url = xdai_rpc_url
        self.polygon_rpc_url = polygon_rpc_url
        self.poap_contract_addrress = poap_contract_address
        self.db_credentials = db_credentials

    def set_endpoints(self):
        """
        Set the RPC endpoints for queries
        """
        self.w3e = Web3(Web3.HTTPProvider(self.eth_rpc_url))
        self.w3x = Web3(Web3.HTTPProvider(self.xdai_rpc_url))

        self.w3p = Web3(Web3.HTTPProvider(self.polygon_rpc_url))
        self.w3p.middleware_onion.inject(
            geth_poa_middleware, layer=0
        )  # adding this due to errors when running getLogs
        return (
            self.w3e.isConnected() and self.w3x.isConnected() and self.w3p.isConnected()
        )

    def create_contract_instance(
        self, web3_provider: Web3, contract_address: str, abi_path: str
    ):
        """
        Instantiate one contract
        """

        if not os.path.exists(abi_path):
            raise OSError(
                "You should have a the abi json file with this path > data_gather/poap_abi.json"
            )
        with open(abi_path, "r") as file:
            abi = file.read()

        contract_instance = web3_provider.eth.contract(
            address=contract_address, abi=abi
        )

        return contract_instance

    def export_to_json_file(
        self,
        content_to_export: object,
        filename_without_extension: str,
        dir_to_export_to: str = "analysis/datasets/",
    ):

        final_path = os.path.join(
            os.getcwd(), dir_to_export_to, f"{filename_without_extension}.json"
        )
        if os.path.exists(final_path):
            print("This file already exists ;).")
        else:
            with open(final_path, "w") as outfile:
                json.dump(content_to_export, outfile)

    def scrappe_erc20token_holders_balance(
        self, web3_provider: Web3, contract_address: str, abi_path: str
    ):
        contract = self.create_contract_instance(
            web3_provider=web3_provider,
            contract_address=contract_address,
            abi_path=abi_path,
        )
        token_symbol = contract.functions.symbol().call()

        print(f"\nGetting all the {token_symbol} holders balances.")

        transfer_logs = utils.fetch_transfer_logs(web3_provider, contract)
        zerox = "0x0000000000000000000000000000000000000000"

        checked_addresses = set()
        transaction_history = []
        for transfer in transfer_logs:

            if transfer["from"] != zerox:
                checked_addresses.add(transfer["from"])

            if transfer["to"] != zerox:
                checked_addresses.add(transfer["to"])

            transaction_history.append({**transfer})

        all_balances = []
        for holder_address in checked_addresses:
            balance = contract.functions.balanceOf(holder_address).call()
            all_balances.append({"holder_address": holder_address, "balance": balance})

        print(f"Done with {token_symbol} holders. \n ")

        return all_balances, token_symbol, transaction_history
