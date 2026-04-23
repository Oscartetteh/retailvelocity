# RetailVelocity — Plain-English Explainer

This is a companion to the README written for people who aren't engineers. No
code, no jargon unless I explain it. Think of it as a guided tour: "what is
this project, what does each piece do, and why should I care?"

## The big picture

Imagine you run an online store. Every day, tens of thousands of people buy
things, and every purchase leaves a record: who bought what, when, for how
much. Those records pile up fast — millions of rows after a year or two.

Buried in that pile are answers to questions that directly make or cost money:

- **Which customers are about to stop buying from us?** Reach them with a
  win-back offer before they leave.
- **Which products will sell out next month?** Order more stock now so we
  don't lose sales.
- **Which products are sitting in the warehouse and not selling?** Mark them
  down or stop ordering them.
- **What time of day and day of week do people actually shop?** Send emails
  when inboxes are open, not when they aren't.

RetailVelocity is a single application that turns a raw transaction log into
answers to all those questions — and shows them in a web dashboard that
anyone on the team can open and click through.

## The parts — what each piece does

### 1. The data generator

Real customer data is private and hard to share, so the project ships with a
**fake dataset** that behaves like a real one. It invents 50,000 shoppers,
2,000 products across eight categories (electronics, beauty, books, and so
on), and about a million purchases spread over three years.

It's not just random noise. The fake data follows the same patterns real
retail data does:

- Sales spike around Black Friday and Christmas.
- Weekends are busier than weekdays.
- A small number of popular products account for most sales, with a long tail
  of rarely-bought items (this is called the 80/20 rule).
- Some customers are whales who buy often, most buy once or twice a year.

Because the patterns are realistic, the analytics and forecasts produce
believable answers — and because the data is fake, anyone can run the project
without any privacy concerns.

### 2. Ingestion — reading the data quickly

"Ingestion" is just a fancy word for **loading the data from disk so the rest
of the program can use it.** That sounds boring, but it matters for a
non-obvious reason: when you're working with millions of rows, loading slowly
means every question you ask takes minutes to answer, and people stop asking.

This project uses a tool called **Polars** (more on why that matters at the
end). Loading a million-row file takes about 50 milliseconds — fast enough
that the dashboard feels instant even on a laptop.

### 3. Descriptive analytics — "what happened?"

This is the simplest kind of analysis: summarising the past.

- **Revenue over time** — total sales each day, week, or month. A line chart
  that shows the business's heartbeat.
- **Rolling average** — smooths the jagged daily numbers into a trendline, so
  you can tell whether sales are actually growing or just bouncing around.
- **By category** — which product types bring in the most money? Which have
  the best profit margins?
- **By country** — where are the customers?
- **Time-of-day heatmap** — a colourful grid showing which hours on which
  weekdays are busiest. Directly useful: send your marketing emails when
  people are already shopping.

These are not rocket science, but they answer "how's the business doing?" for
a stakeholder who doesn't want to dig into a spreadsheet.

### 4. RFM customer segmentation — "who are our best customers?"

RFM stands for **Recency, Frequency, Monetary** — three questions about every
customer:

- **Recency** — how long since they last bought something?
- **Frequency** — how often do they buy?
- **Monetary** — how much have they spent in total?

Each customer gets a score on each dimension. Combine the three scores and
you can sort every customer into one of four buckets:

- **Platinum** — recent buyers, frequent buyers, big spenders. Your best.
- **Gold** — strong on most dimensions. Nurture them.
- **Silver** — moderate. Could go either way.
- **Bronze** — haven't bought in a while, or rarely buy, or spend little.

Two business outcomes come out of this:

1. **Know who to reward.** Platinum customers get VIP treatment, early access,
   thank-you perks. They're the ones keeping the lights on.
2. **Know who to win back.** The dashboard produces a "high-value but gone
   quiet" list — customers who used to spend a lot but haven't purchased in
   months. These are your highest-leverage targets for a discount email or a
   "we miss you" campaign.

In the fake data, the top 31% of customers (Platinum) generate **64% of all
revenue** — a clean illustration of the 80/20 rule, and a sharp reminder of
who not to lose.

### 5. Cohort retention — "do customers come back?"

A **cohort** is a group of customers who signed up in the same month. Cohort
analysis asks: if 1,000 people signed up in January, how many of them are
still buying from us in February? In March? Six months later?

Plotted as a heatmap, this tells a story:

- A healthy business has colour fading slowly from left to right — customers
  keep coming back month after month.
- A leaky business has colour that drops off fast after the first month —
  people buy once and never return.
- A business that got worse over time has newer rows (cohorts) that retain
  less well than older ones — a warning sign the product experience or
  marketing has degraded.

This is the single most honest chart you can show an investor about whether
your customers actually like you.

### 6. Forecasting — "what will happen next month?"

Descriptive analytics looks backward. Forecasting looks forward. For the top
products (by revenue), the system fits a statistical model to the daily sales
history and projects the next 30 days.

Two things come out:

- **A predicted demand number** — e.g. "we'll sell about 420 units of this
  SKU next month."
- **A confidence band** — e.g. "…but it could be anywhere from 350 to 490."
  The band is wider when the product's sales are noisy, narrower when they're
  stable. This tells the supply-chain team how much safety stock to carry.

It also reports a **MAPE** score (Mean Absolute Percentage Error) — basically
"how wrong was the forecast last month?" On the fake dataset, the top SKU
forecast comes in around 10% error, which is usable.

### 7. Prescriptive — "what should we do about it?"

This is where forecasts become to-do lists. Three outputs:

- **Reorder report** — for every top product, show the expected demand over
  the lead time (the days it takes to get more stock delivered), add a safety
  buffer, and compare to what's currently in the warehouse. Each SKU gets a
  traffic light:
  - 🔴 **Red** — you'll run out before the next shipment arrives.
  - 🟡 **Yellow** — tight. Reorder now to be safe.
  - 🟢 **Green** — you're fine.
- **Dead stock list** — products that haven't sold in the last six months
  but still have inventory sitting in the warehouse. Every one of those is
  money locked up in something that isn't moving.
- **At-risk revenue** — the total dollar value of the "high-value gone quiet"
  customer list. Usefully scary: a single number that says "if we don't
  win these customers back, we stand to lose $X."

### 8. Performance benchmarks — "how fast is it, really?"

A lot of analytics tools claim to be fast. This project **measures and reports
its own speed** as a first-class feature, so the claim is checkable. A
dedicated benchmark module times two workloads — a typical sales
group-by-and-join, and the full customer-segmentation pipeline — on the
current dataset and prints a table of seconds and rows-per-second.

On a standard laptop with a million rows, the group-by finishes in about 19
milliseconds, and the full segmentation finishes in about 11 milliseconds.
Those are the numbers quoted in the README. Anyone who clones the repo can
re-run the benchmark on their own machine and check for themselves.

Why does this matter for a portfolio piece? Because "fast" on a resume is
cheap talk. Numbers you can reproduce on your own laptop are not.

### 9. The dashboard (Streamlit)

Everything above is available through a **web dashboard** — open the app in a
browser, click through pages, move sliders. No SQL, no Python, no spreadsheet.

Pages:

- **Home** — top-line numbers (revenue, profit, orders, margin).
- **Trends** — the time-series charts and heatmap.
- **RFM Segments** — tier breakdown, customer scatter plot, win-back list.
- **Cohort Retention** — the retention heatmap.
- **Forecast** — pick a product, see its history plus the 30-day forecast.
- **Prescriptive** — the reorder traffic-light report, dead stock, at-risk
  dollars.

The whole app is one command to launch and works on any laptop.

### 10. The command-line tool

Not everyone wants a dashboard. For engineers and data teams who prefer the
terminal, the project ships a small command-line program called
`retailvelocity` with three commands:

- `retailvelocity generate` — create the synthetic dataset.
- `retailvelocity summary` — print the top-line KPIs (revenue, customers,
  orders, date range) in one short block.
- `retailvelocity benchmark` — run the speed benchmarks described above.

This is useful for running the project inside a nightly job, a CI pipeline,
or a Docker container where opening a browser isn't practical.

### 11. The walkthrough notebook

Inside the `notebooks/` folder is a **Jupyter notebook** — an interactive
document that mixes explanation and live code. It runs every module in order
on the live dataset: load the data, compute trends, segment customers, run a
cohort analysis, fit a forecast, produce the reorder report, and print the
benchmark table.

For a technical reader who wants to understand *how* the project works
step-by-step (rather than just clicking around in the dashboard), the
notebook is the readable story. It also renders cleanly on GitHub, so
recruiters can skim it without installing anything.

### 12. Tests

In software, "tests" are small automated programs that check the real program
still works. Every time the code changes, all the tests run and flag
anything broken. This project has 27 tests that run in under two seconds,
covering every analysis step. Recruiters and engineers reading the repo can
see that the code works, because the tests say so.

### 13. Continuous integration (CI)

Whenever code gets pushed to GitHub, a cloud service automatically runs the
tests and the lint checks. A green checkmark next to every commit means the
code was correct at that point in history. Red means something broke and
needs fixing. It's a quality signal you don't have to do anything to produce
— it's automatic.

## Why "Polars"?

Most data-science projects use a tool called **Pandas** to crunch data. It
works, but it's slow on big data and uses a lot of memory. **Polars** is a
newer, faster alternative built from the ground up to be quick and to scan
files without loading them entirely into memory.

On this project, Polars processes **a million rows in about 20 milliseconds**
(that's 0.02 seconds) for a typical grouping-and-counting operation. That's
fast enough that the dashboard feels instant even on a laptop — which in turn
means people *actually use* the dashboard. Speed isn't vanity; it's what
makes an analytics tool get adopted instead of gathering dust.

## Why this project matters (the elevator pitch)

- It turns a million rows of messy transaction history into a dashboard the
  business can actually look at.
- It shows which customers to keep, which to win back, and which are gone.
- It predicts what will sell next month, and warns when a product is about
  to run out or is sitting dead in the warehouse.
- It runs fast enough to be used daily, not quarterly.
- It's end-to-end: data, analysis, prediction, dashboard, tests, and
  automated quality checks — all in one repository.

For a business, that's the difference between reacting to last quarter's
numbers and steering the ship in real time.
