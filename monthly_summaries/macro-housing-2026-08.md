# August 2026 — The Freeze Beneath the Soft Landing

*Macro narrative with housing read-through. Data through 2026-08-25.*

> **Data gaps in this draft:** Altos weekly (pending sales, inventory, new listings) and the
> Xactus Mortgage Intent Index last captured 2026-06-29 — observations end 2026-06-26. Redfin
> last captured 2026-06-03. Sections marked `[ALTOS]`, `[XACTUS]`, and `[REDFIN]` are left for
> Mike to fill. Every other number below is live from Postgres.

---

## The thesis

Nearly every August release improved on the measures the Fed watches and deteriorated on the
measures that generate housing transactions. Both readings are correct. They are measuring
different things, and the gap between them is the story of the month.

The Fed sees core inflation at 2.47%, unemployment at 4.1% and falling, and initial claims 12%
below last year. That is a soft landing. Housing sees a quits rate of 2.0%, a labor force
shrinking 0.77% year over year, and existing-home inventory turning negative for the first time
in 2026 — not because buyers absorbed it, but because sellers stopped listing. That is a market
where almost nobody has a reason to move.

The common thread across the August data is that **levels are holding and flows are freezing.**
Nobody is being fired, and nobody is moving. Builders are permitting and not building. Sellers
are withdrawing rather than cutting. Every one of those frozen flows is a housing transaction
input.

---

## 1. The labor market is not weak. It is still.

July payrolls came in at 158,858k against 158,881k in June — a net loss of 23,000 jobs, and
growth of just 0.20% year over year. The unemployment rate simultaneously fell to 4.1%, down from
4.3% as recently as March.

Those two facts only reconcile one way, and it is the important one. The civilian labor force fell
to 169,094k, down 0.77% year over year. Participation is 61.4%, down from 61.9% in March. The
household survey's employment level is 162,177k, down 0.59% year over year. **The unemployment
rate improved because the denominator shrank, not because jobs appeared.**

Underneath that, the flow data is where housing lives:

| Measure | Latest | Reading |
|---|---|---|
| Initial claims (8/15) | 206,000 | −11.97% YoY; 4-wk avg 204,000 |
| JOLTS hires rate (June) | 3.4% | Historically depressed |
| JOLTS quits rate (June) | 2.0% | Historically depressed |
| JOLTS openings (June) | 7,359k | −2.36% MoM, +2.15% YoY |
| U-6 (July) | 7.9% | Down from 8.2% in April |
| Unemployed 27+ weeks (July) | 1,771k | −8.57% MoM, −2.8% YoY |

This is a low-firing, low-hiring market. Claims at 206k say employers are holding onto people. A
quits rate of 2.0% says workers are not testing the market. **A job change is the single largest
trigger for a household move, and that trigger is not firing.** No amount of rate relief
substitutes for it.

One genuine improvement worth flagging: the long-term unemployed cohort fell for the second
straight month to 1,771k and is now *below* year-ago levels at −2.8%. In May that same series was
running +35.8% year over year. That reversal takes the future distressed-borrower story off the
table for now — it was the one labor series that looked like it was building toward 2027 trouble.

Wages are decelerating but not collapsing. Average hourly earnings are +3.15% year over year, down
from +3.41% in June, with a July gain of just +0.05% on the month. The Atlanta Fed's 3-month wage
tracker actually ticked up to 3.8% from 3.6%. Real disposable income per capita is +0.27% year over
year — effectively flat. Buyers are not gaining purchasing power from income.

---

## 2. Inflation is at target once you take out the pump.

| Measure | Latest | YoY |
|---|---|---|
| Core CPI (July) | +0.22% MoM | **+2.47%** |
| Headline CPI (July) | +0.07% MoM | +3.30% |
| Trimmed-mean PCE (June) | — | **+2.23%** |
| Core PCE (June) | +0.13% MoM | +3.29% |
| Headline PCE (June) | −0.11% MoM | +3.67% |
| PPI final demand (July) | −0.03% MoM | +4.66% (from +5.54%) |
| CPI energy (July) | −1.48% MoM | **+14.45%** |

Core CPI at 2.47% is down from 2.82% in May and is, for practical purposes, at the Fed's target.
The Dallas Fed's trimmed-mean PCE at 2.23% says the same thing with a different method. The entire
gap between headline and core is energy: CPI energy is +14.45% year over year and WTI is +27.5%
year over year at $82.64.

That gap is closing on its own. WTI fell 2.79% on August 25 and is down from $87.83 on August 20.
Energy CPI has now fallen two consecutive months.

**More importantly, the market has already looked through it.** Five-year, ten-year, and
five-year-five-year-forward breakevens have all converged at roughly 2.32% — every horizon pricing
the same number, all below year-ago levels. The Cleveland Fed's one-year expected inflation dropped
to 2.39% in July from 3.04% in June and held there in August. Nobody is pricing an energy spike
into medium-term inflation.

The housing-relevant component is behaving best of all. **Shelter CPI is +3.16% year over year,
decelerating for a third straight month** (3.35% → 3.26% → 3.16%), with a July gain of just +0.14%.
And the leading indicator points further down: Zillow's ZORI has rent at +3.0% year over year in
July, down from +3.34% in June, and actually −0.14% on the month. Measured shelter lags market
rents by 12–18 months, so the shelter contribution to core has further to fall mechanically.

Shelter deceleration is what carried core CPI to 2.47%. It is the single most useful inflation fact
of the month.

---

## 3. August's rate relief came from plumbing, not policy.

This is the cleanest mechanism in the month's data, and it is invisible if you only watch the
headline mortgage rate.

| Date | 10Y Treasury | 30Y Fixed | Spread |
|---|---|---|---|
| 7/27 | 4.64% | 6.80% | 2.16 |
| 8/03 | 4.69% | 6.82% | 2.13 |
| 8/13 | 4.62% | 6.69% | 2.07 |
| 8/18 | 4.74% | 6.75% | **2.01** |
| 8/25 | 4.66% | **6.74%** | 2.08 |

Over August the 10-year Treasury went essentially nowhere — 4.69% to 4.66%, and it is *up* 0.38
percentage points year over year. The 30-year fixed still fell about 8 basis points. All of that
came from spread compression: 2.16 in late July to a low of 2.01 on August 18, now 2.08.

The Fed is not the actor here either. Effective fed funds is 3.63%, already down 0.70 percentage
points year over year — the cuts have been delivered, and the long end went *up* anyway.

The mortgage-Treasury spread — the premium lenders and investors charge over the risk-free rate —
historically runs around 1.7 percentage points. At 2.08 there is still roughly 35–40 basis points
of normalization available. **If the spread returns to 1.85–1.90 with the 10-year where it sits
today, the 30-year prints in the low 6s with no Fed action and no change in the economy.** That is
the most underrated source of affordability improvement available to this market, and it is the one
nobody's clients are watching.

---

## 4. Builders are buying optionality, not building houses.

The starts-versus-permits divergence flipped this month, and the new direction is more informative
than the old one.

| Measure (July) | Level | YoY |
|---|---|---|
| Building permits | 1,433k | **+5.21%** |
| Single-family permits | 894k | +2.17% |
| Housing starts | 1,239k | **−12.75%** |
| Single-family starts | 808k | −15.04% |
| Completions | 1,212k | −16.70% |
| Under construction | 1,262k | −6.03% |
| Residential construction spending (June) | $889.4B | −2.64% |

Permits up 5%, starts down 13%, completions down 17%. Builders are pulling entitlements and
declining to break ground.

The employment data confirms it is deliberate. Residential building employment is 914.6k, down
1.45% year over year and falling for four straight months. Nonresidential building employment is
953.3k, **up 3.0%** and rising. Total construction employment is up 0.99% — the aggregate looks
fine because crews are moving from houses to everything else.

Permits are cheap; starts are capital and labor. Builders are keeping the pipeline alive at minimum
cost while refusing to commit. That is a rational read on their own demand, and it shows up in the
new-home numbers: 607k SAAR in July, down 5.01% year over year, with 9.6 months of supply against
4.6 months for existing homes. The new-home median is $393,800, down 0.88% year over year — builders
are buying the sale with price and incentive.

**Read-through: this is a 2027 supply story, not a 2026 one.** Completions down 17% and units under
construction down 6% means the delivery pipeline thins out roughly a year from now.

---

## 5. Supply is tightening — for the wrong reason.

| Measure | Latest | YoY |
|---|---|---|
| Existing-home inventory (July) | 1,540k | **−0.65%** |
| Realtor.com new listings (July) | 423,732 | −2.55% |
| Realtor.com active listings (July) | 1,126,252 | +2.13% |
| Realtor.com pending (July) | 469,513 | +1.90% |
| Zillow for-sale inventory (July) | 1,389,936 | +1.21% |
| Realtor.com price-reduced count (July) | 404,344 | −2.24% |
| Realtor.com median DOM (July) | 57 days | −1.72% |

July is the first negative year-over-year inventory print of 2026, after seven consecutive positive
months. Zillow's inventory growth has decelerated from +5.25% in March to +1.21% in July. Realtor.com
new listings turned negative at −2.55%.

**This is not absorption. It is withdrawal.** Sales are not strong enough to have eaten the supply —
existing-home sales ran 4.06M SAAR in July, up just 1.25% year over year and decelerating from
+4.07% in June. Inventory is falling because listings stopped arriving. That is the same freeze as
the labor data, viewed from the seller's side: the household that would have listed did not get the
job offer, so it stayed put.

The price-reduced count falling 2.24% year over year fits the same pattern. Sellers who would have
been forced to cut are instead not listing at all.

Prices are holding as a result. The existing-home median is $434,100, +1.97% year over year. But
affordability keeps deteriorating anyway: the median-price-to-median-income ratio is 5.185, up 1.99%
year over year. Flat rates plus flat incomes plus rising prices still equals a worse entry point
than a year ago.

`[ALTOS]` — weekly pending sales streak, 13-week trend, total active supply, new listings
acceleration, and pending days on market go here. The last captured week is 6/26 (pending +4.81%
YoY on the 13-week, DOM 70.9).

`[REDFIN]` — sale-to-list, price drop share, off-market-in-two-weeks.

---

## 6. The two-speed price map is real, and two datasets agree on it.

Case-Shiller, May reference period (releases run ~2 months behind), national index +1.05% YoY:

**Appreciating:** Chicago +6.91% · New York +4.03% · Cleveland +3.12% · Detroit +2.98% (April ref) ·
Boston +2.64% · San Francisco +2.19% · Minneapolis +1.82% · Miami +1.79%

**Declining:** Las Vegas −1.89% · Seattle −1.77% · Tampa −1.76% · Denver −1.70% · Phoenix −1.34% ·
Dallas −1.02% · Portland −0.85% · Atlanta −0.11%

That is a spread of roughly 8.8 points between Chicago and Las Vegas within a national index sitting
at +1%. A national price number is close to useless right now.

NAR's regional medians for July, an entirely independent dataset, tell the same story: Northeast
+5.25% year over year, Midwest +2.85%, South +0.92%, West +0.24%. Two sources, same split — this is
not a Case-Shiller construction artifact.

The mechanism is supply, not demand. The Sun Belt metros built through the last cycle and are now
absorbing that inventory. The Midwest and Northeast did not build and have nothing to absorb. An
agent in Chicago and an agent in Tampa need opposite price narratives this fall, and both are
supported by the data.

---

## 7. The consumer is the crack in the soft-landing story.

| Conference Board (July) | Level | YoY |
|---|---|---|
| Headline confidence | 90.8 | −8.0% |
| Present Situation | 114.9 | **−13.48%** |
| Expectations | 74.7 | −1.71% |

Headline confidence has fallen three straight months. The composition matters more than the level:
consumers rate *today* 13.5% worse than a year ago while rating the *future* roughly unchanged.

Present-situation readings track labor market flow — how easy it feels to get a job, change a job,
get a raise — rather than the unemployment rate. A 4.1% unemployment rate with a 2.0% quits rate
feels exactly like this. It is the household-level expression of the same freeze, and it is the one
series in this month's data that flatly contradicts the soft-landing read.

---

## What to watch next

1. **July PCE, due ~August 28.** Core PCE was +3.29% year over year in June while core CPI printed
   +2.47% in July. That 80-basis-point gap has to close in one direction or the other. If core PCE
   follows CPI down, the Fed's own preferred gauge lands near target and the September meeting gets
   interesting. This is the highest-value single release of the next two weeks.

2. **The spread.** Whether 2.01 on August 18 was a floor or a first step. Every 10 basis points of
   compression is worth roughly 10 basis points off the 30-year with no help from anyone.

3. **August payrolls, early September.** Specifically the labor force and participation lines, not
   the headline. If participation keeps falling, the unemployment rate keeps "improving" while
   housing demand keeps freezing — and the Fed keeps reading it as strength.

4. **Case-Shiller June reference**, due imminently. Watch whether the Chicago/Sun Belt spread widens
   past 9 points.

5. `[XACTUS]` — MII leads MBA purchase applications by 2–4 weeks. Worth noting that MBA has been
   deteriorating without it: the 13-week average is +0.83% year over year as of 8/14, down from
   +5.2% in mid-July, with the 4-week at −1.2% and the spot week at −3.43%. Purchase demand is
   rolling over on the one leading series still reporting.
