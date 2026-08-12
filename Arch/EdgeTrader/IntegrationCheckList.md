# Integration Checklist

- [X] ~~*Redesign Dedup Mechanics*~~ [2026-08-06]
- [X] ~~*BINANCE_SYMBOL and BINANCE_BAR_INTERVAL*~~ [2026-08-06]
- [X] ~~*Check other env and add debug message about the current mode*~~ [2026-08-07]
- [X] ~~*Learn how to check manually*~~ [2026-08-07]
- [X] ~~*Successfully process open message*~~ [2026-08-12]
- [ ] REST API for getting stratagies (orders)
- [ ] Add open check in CI/CD
- [ ] Make sure we can open multiple simultanious trades
- [ ] Successfully process cancel message
- [ ] Add cancel check in CI/CD
- [ ] All the state machine stransitions do there job
- [ ] I want another postgres table the same as google sheet for closed trades
- [ ] Separate insert function for each event
- [ ] Solve missing SQS messages

## Corner Cases

- [ ] Old outdated sl, ep, tp - price <= sl, price >= tp
- [ ] Two different orders if price <= ep and price>=ep
