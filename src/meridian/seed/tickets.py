"""Support ticket corpus — the RAG layer's source data.

Generated from an intent grammar rather than sampled from a fixed list, so the
corpus has genuine lexical variety. Retrieval evaluated against near-duplicate
text produces meaningless recall numbers.

Every ticket carries a ground-truth `intent`, which serves two purposes: it is the
label the golden eval scores retrieval against, and it is the comparison baseline
for the LLM classification in Phase 1's enrichment path.

Ticket bodies deliberately embed customer names, emails and order references.
That is the point — the PII masking in Phase 1 has to survive real text, and
every value used here is in the manifest that masking reads from.
"""

from __future__ import annotations

import datetime as dt
import random

from . import config as C

# Each intent gets several subject lines and several body templates. Slots are
# filled from the customer and order the ticket actually belongs to.
GRAMMAR: dict[str, dict[str, list[str]]] = {
    "shipping_delay": {
        "subjects": [
            "Where is order {order_id}?",
            "Order {order_id} still hasn't arrived",
            "Delivery is late",
            "No tracking update in {days} days",
        ],
        "bodies": [
            "Hi, this is {first}. I placed order {order_id} on {order_date} and the tracking "
            "hasn't moved in {days} days. It was supposed to arrive last week. Can someone "
            "tell me where it actually is? You can reach me at {email}.",
            "My order {order_id} is showing as shipped but nothing has been delivered. It's "
            "been {days} days now. I need this before the weekend — is it lost?",
            "{first} {last} here. Order {order_id} was promised in 3-5 days and we're well past "
            "that. The carrier page just says 'label created'. Please advise.",
            "Still waiting on {order_id}. This is the second time an order to my address has "
            "stalled. Getting frustrated — can you escalate this?",
        ],
    },
    "refund_request": {
        "subjects": [
            "Refund for order {order_id}",
            "Requesting refund",
            "Please refund {order_id}",
            "Charged but want to cancel",
        ],
        "bodies": [
            "I'd like a refund for order {order_id}. It arrived and it isn't what I expected — "
            "the listing photos are misleading. My email on the account is {email}.",
            "Please refund order {order_id} placed {order_date}. I cancelled within the window "
            "but the charge still went through on my card.",
            "Hi — {first} here. I need to return the item from {order_id} and get my money back. "
            "What's the process? I still have the original packaging.",
            "Order {order_id} was a duplicate — I was charged twice for the same thing. Please "
            "refund one of them. Contact me at {email} if you need details.",
        ],
    },
    "product_defect": {
        "subjects": [
            "Item arrived damaged",
            "Defective product in {order_id}",
            "Broken on arrival",
            "Product stopped working",
        ],
        "bodies": [
            "The item from order {order_id} arrived with a cracked casing. It was clearly "
            "damaged before shipping — the inner packaging was intact. I'd like a replacement.",
            "This is {first} {last}. The unit I received in {order_id} powers on but cuts out "
            "after about ten minutes. It's been {days} days and it's getting worse.",
            "Product from {order_id} is defective straight out of the box. Nothing happens when "
            "I plug it in. I've tried a different outlet and cable. Please replace it.",
            "Received order {order_id} and one of the items is faulty. Photos attached. Reachable "
            "at {email} or {phone}.",
        ],
    },
    "billing_question": {
        "subjects": [
            "Question about my charge",
            "Unexpected amount on {order_id}",
            "Billing discrepancy",
            "Why was I charged this?",
        ],
        "bodies": [
            "I was charged {amount} for order {order_id} but the checkout page showed a lower "
            "total. Where did the difference come from? Was tax added afterwards?",
            "There's a charge on my statement I don't recognise, referencing {order_id}. Could "
            "you break down what it covers? My account email is {email}.",
            "Hi, {first} here — the discount code I applied to {order_id} doesn't seem to have "
            "come off the total. Can you check and adjust?",
            "Getting billed twice for what looks like the same order ({order_id}). Please look "
            "into it.",
        ],
    },
    "account_access": {
        "subjects": [
            "Can't log in",
            "Locked out of my account",
            "Password reset not working",
            "Login issue",
        ],
        "bodies": [
            "I can't get into my account. The password reset email never arrives — I've checked "
            "spam. The address is {email}. Can you reset it manually?",
            "{first} {last} here, locked out after too many attempts. I need to check on order "
            "{order_id}. Phone is {phone} if that's faster.",
            "My account says it doesn't exist but I've ordered from you before, most recently "
            "{order_id} on {order_date}. Did something get merged or deleted?",
            "Two-factor is sending codes to an old number. I no longer have access to it. How do "
            "I recover the account tied to {email}?",
        ],
    },
    "return_process": {
        "subjects": [
            "How do I return this?",
            "Return label for {order_id}",
            "Return instructions",
            "Started a return, no label",
        ],
        "bodies": [
            "I started a return for order {order_id} but never got the shipping label. It's been "
            "{days} days. Can you resend it to {email}?",
            "What's the return window on {order_id}? I bought it on {order_date} and I'm not "
            "sure if I'm still inside it.",
            "Hi — need to send back one item from {order_id}, not the whole order. Is that "
            "possible or does it have to go back together?",
            "Do I pay return shipping on {order_id}? The policy page wasn't clear about which "
            "cases are covered.",
        ],
    },
    "general_inquiry": {
        "subjects": [
            "Question about a product",
            "Do you ship to my area?",
            "Stock question",
            "Quick question",
        ],
        "bodies": [
            "Is the item from order {order_id} coming back in stock? I'd like to buy a second "
            "one for a gift.",
            "Do you ship to rural addresses? My last order ({order_id}) went through fine but I'm "
            "moving and want to check before I order again.",
            "Hi, this is {first}. Is there a bulk discount if I order more than five units? "
            "Reachable at {email}.",
            "What's the warranty on the items in {order_id}? The product page mentions coverage "
            "but doesn't say how long.",
        ],
    },
}

# Priority is a function of intent — a defect or an access lockout is more urgent
# than a stock question. Weights are per intent so the distribution is realistic.
PRIORITY_BY_INTENT = {
    "shipping_delay": {"P2": 0.25, "P3": 0.55, "P4": 0.20},
    "refund_request": {"P2": 0.35, "P3": 0.50, "P4": 0.15},
    "product_defect": {"P1": 0.18, "P2": 0.47, "P3": 0.30, "P4": 0.05},
    "billing_question": {"P2": 0.30, "P3": 0.55, "P4": 0.15},
    "account_access": {"P1": 0.22, "P2": 0.45, "P3": 0.28, "P4": 0.05},
    "return_process": {"P3": 0.62, "P4": 0.38},
    "general_inquiry": {"P3": 0.35, "P4": 0.65},
}

INTENT_WEIGHTS = {
    "shipping_delay": 0.26,
    "refund_request": 0.18,
    "product_defect": 0.16,
    "billing_question": 0.14,
    "account_access": 0.10,
    "return_process": 0.11,
    "general_inquiry": 0.05,
}


def make_tickets(
    rng: random.Random,
    customers: list[dict],
    orders: list[dict],
    anchor: dt.date,
) -> list[dict]:
    by_customer_id = {c["customer_id"]: c for c in customers}
    n_tickets = int(len(orders) * C.TICKETS_PER_100_ORDERS / 100)

    tickets: list[dict] = []
    for i in range(1, n_tickets + 1):
        order = rng.choice(orders)
        cust = by_customer_id[order["customer_id"]]

        intent = rng.choices(list(INTENT_WEIGHTS), weights=list(INTENT_WEIGHTS.values()), k=1)[0]
        grammar = GRAMMAR[intent]

        order_date = dt.date.fromisoformat(order["order_date"])
        # Tickets are raised after the order, within a plausible window, and never
        # in the future relative to the anchor.
        max_lag = min((anchor - order_date).days, 45)
        if max_lag < 1:
            continue
        created = order_date + dt.timedelta(days=rng.randint(1, max_lag))
        days = rng.randint(3, 21)

        slots = {
            "first": cust["first_name"],
            "last": cust["last_name"],
            "email": cust["email"],
            "phone": cust["phone"],
            "order_id": order["order_id"],
            "order_date": order["order_date"],
            "amount": f"${order['total_amount']:.2f}",
            "days": days,
        }

        subject = rng.choice(grammar["subjects"]).format(**slots)
        body = rng.choice(grammar["bodies"]).format(**slots)

        priorities = PRIORITY_BY_INTENT[intent]
        priority = rng.choices(list(priorities), weights=list(priorities.values()), k=1)[0]

        # Sentiment correlates with intent and priority, which gives the AI
        # enrichment something real to be scored against.
        if intent in ("product_defect", "shipping_delay") or priority == "P1":
            sentiment = rng.choices(["negative", "neutral"], weights=[0.78, 0.22], k=1)[0]
        elif intent == "general_inquiry":
            sentiment = rng.choices(["neutral", "positive"], weights=[0.7, 0.3], k=1)[0]
        else:
            sentiment = rng.choices(["negative", "neutral", "positive"], weights=[0.45, 0.42, 0.13], k=1)[0]

        resolved = rng.random() < 0.82
        tickets.append(
            {
                "ticket_id": f"T{i:06d}",
                "customer_id": cust["customer_id"],
                "order_id": order["order_id"],
                "created_ts": f"{created.isoformat()}T{rng.randint(7, 20):02d}:{rng.randint(0, 59):02d}:00+00:00",
                "resolved_ts": (
                    f"{(created + dt.timedelta(days=rng.randint(1, 9))).isoformat()}"
                    f"T{rng.randint(7, 20):02d}:00:00+00:00"
                    if resolved
                    else ""
                ),
                "status": "resolved" if resolved else rng.choice(["open", "pending"]),
                "channel": rng.choice(["email", "chat", "phone", "web_form"]),
                "subject": subject,
                "body": body,
                # Ground truth. The LLM enrichment in Phase 1 predicts these
                # independently and is scored against them.
                "intent": intent,
                "priority": priority,
                "sentiment": sentiment,
            }
        )

    tickets.sort(key=lambda t: t["created_ts"])
    return tickets
