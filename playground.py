# import asyncio

# # On 3.11+, SIGINT often cancels the running task's current `await` with
# # CancelledError instead of delivering KeyboardInterrupt into `except`.
# # Catch both so each Ctrl+C is handled reliably.
# _Signal = (KeyboardInterrupt, asyncio.CancelledError)


# async def counter() -> None:
#     """First cancel: swallow once. Second cancel: propagate and exit."""
#     n = 0
#     while True:
#         try:
#             await asyncio.sleep(1)
#             print("zzzz")
#         except asyncio.CancelledError:
#             if n > 0:
#                 raise
#             print("nice try!")
#             n += 1


# async def main() -> None:
#     t = asyncio.create_task(counter())
#     try:
#         while True:
#             await asyncio.sleep(1)
#     except _Signal:
#         print("[1] first Ctrl+C — cancel (counter hijacks the first one)")
#         t.cancel()

#     try:
#         while True:
#             print(f"cancelled??? {t.cancelled()}")
#             await asyncio.sleep(1)
#     except _Signal:
#         print("[2] second Ctrl+C — force exit")
#         t.cancel()
#         try:
#             await t
#         except asyncio.CancelledError:
#             pass
#         print(f"after await: cancelled={t.cancelled()}  done={t.done()}")


# if __name__ == "__main__":
#     asyncio.run(main())
