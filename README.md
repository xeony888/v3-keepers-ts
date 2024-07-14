Multithreaded liquidity bot

Uses Workers to handle multiple threads. 
Accounts are sorted by priority level - accounts closest to liquidation are checked more often

Users can change compute limit fee (gas fee) in utils.ts

Check package.json for commands to run the liquidator

Install and run redis before operating. Redis is used to share data between threads in a simple way

