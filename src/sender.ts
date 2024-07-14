import {
  BlockhashWithExpiryBlockHeight,
  ComputeBudgetProgram,
  Connection,
  Signer,
  Transaction,
  TransactionExpiredBlockheightExceededError,
} from "@solana/web3.js";
import promiseRetry from "promise-retry";


const wait = async (ms: number) => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};
const SEND_OPTIONS = {
  skipPreflight: true,
};

export async function sendTransaction(
  connection: Connection,
  transaction: Transaction,
  blockhashWithExpiryBlockHeight: BlockhashWithExpiryBlockHeight,
  signers: Signer[],
) {
  // increase compute fee
  transaction.add(
    ComputeBudgetProgram.setComputeUnitPrice({
      microLamports: 10000
    })
  );
  transaction.sign(...signers);
  const txid = await connection.sendRawTransaction(
    transaction.serialize(),
    SEND_OPTIONS
  );

  const controller = new AbortController();
  const abortSignal = controller.signal;

  const abortableResender = async () => {
    while (true) {
      await wait(2_000);
      if (abortSignal.aborted) return;
      try {
        await connection.sendRawTransaction(
          transaction.serialize(),
          SEND_OPTIONS
        );
      } catch (e) {
        console.warn(`Failed to resend transaction: ${e}`);
      }
    }
  };

  try {
    abortableResender();
    const lastValidBlockHeight =
      blockhashWithExpiryBlockHeight.lastValidBlockHeight - 150;

    await Promise.race([
      connection.confirmTransaction(
        {
          ...blockhashWithExpiryBlockHeight,
          lastValidBlockHeight,
          signature: txid,
          abortSignal,
        },
        "confirmed"
      ),
      new Promise((resolve) => {
        while (!abortSignal.aborted) {
          wait(2000).then(async () => {
            const tx = await connection.getSignatureStatus(txid, {
              searchTransactionHistory: false,
            });
            if (tx?.value?.confirmationStatus === "confirmed") {
              resolve(tx);
            }
          });
        }
      }),
    ]);
  } catch (e) {
    if (e instanceof TransactionExpiredBlockheightExceededError) {
      // useless error
      return null;
    } else {
      throw e;
    }
  } finally {
    controller.abort();
  }

  // in case rpc is not synced yet, we add some retries
  const response = promiseRetry(
    async (retry: any) => {
      const response = await connection.getTransaction(txid, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });
      if (!response) {
        retry(response);
      }
      return response;
    },
    {
      retries: 5,
      minTimeout: 1e3,
    }
  );

  return response;
}