# AI Model Comparison: Pricing, Rate Limits & Specifications

*Last updated: February 2026*

---

## Quick Summary Table (Top Models)

| Provider | Model | Input $/MTok | Output $/MTok | Context Window |
|----------|-------|-------------|---------------|----------------|
| **OpenAI** | GPT-4o | $2.50 | $10.00 | 128K |
| **OpenAI** | GPT-4o-mini | $0.15 | $0.60 | 128K |
| **Anthropic** | Claude Opus 4.5 | $5.00 | $25.00 | 200K (1M beta) |
| **Anthropic** | Claude Sonnet 4.5 | $3.00 | $15.00 | 200K (1M beta) |
| **Anthropic** | Claude Haiku 4.5 | $1.00 | $5.00 | 200K |
| **Google** | Gemini 2.5 Pro | $1.25 | $10.00 | 1M |
| **Google** | Gemini 2.5 Flash | $0.30 | $2.50 | 1M |
| **Mistral** | Mistral Large 3 | $0.50 | $1.50 | 262K |
| **Mistral** | Mistral Medium 3.1 | $0.40 | $2.00 | 131K |

---

## OpenAI

### Models & Pricing

| Model | Input $/MTok | Output $/MTok | Context | Notes |
|-------|-------------|---------------|---------|-------|
| **GPT-4o** | $2.50 | $10.00 | 128K | Flagship model |
| **GPT-4o-mini** | $0.15 | $0.60 | 128K | 16x cheaper than GPT-4o |
| **GPT-4** | ~$30.00 | ~$60.00 | 128K | Legacy (much more expensive) |

### Cost-Saving Options
- **Cached input**: $1.25/MTok (50% off)
- **Batch API**: 50% discount on both input and output (24h turnaround)

### Rate Limits
- Rate limits measured in RPM (Requests Per Minute) and TPM (Tokens Per Minute)
- Limits vary by tier and usage level
- Enterprise: Contact sales for custom limits

### Subscription Plans
| Plan | Price | Features |
|------|-------|----------|
| ChatGPT Free | $0 | Limited access |
| ChatGPT Plus | $20/month | Enhanced access, GPT-4o |
| ChatGPT Pro | $200/month | Unlimited access, priority |

**Sources**: [OpenAI Pricing](https://platform.openai.com/docs/pricing), [OpenAI API](https://openai.com/api/pricing/)

---

## Anthropic (Claude)

### Models & Pricing

| Model | Input $/MTok | Output $/MTok | Context | Notes |
|-------|-------------|---------------|---------|-------|
| **Claude Opus 4.6** | $5.00 | $25.00 | 200K (1M beta) | Latest flagship |
| **Claude Opus 4.5** | $5.00 | $25.00 | 200K (1M beta) | Previous flagship |
| **Claude Sonnet 4.5** | $3.00 | $15.00 | 200K (1M beta) | Balanced performance/cost |
| **Claude Sonnet 4** | $3.00 | $15.00 | 200K (1M beta) | |
| **Claude Haiku 4.5** | $1.00 | $5.00 | 200K | Fastest, cheapest |
| **Claude Haiku 3.5** | $0.80 | $4.00 | 200K | |
| **Claude Haiku 3** | $0.25 | $1.25 | 200K | Budget option |

### Long Context Pricing (>200K tokens)
| Model | Input $/MTok | Output $/MTok |
|-------|-------------|---------------|
| Claude Opus 4.6 | $10.00 | $37.50 |
| Claude Sonnet 4.5/4 | $6.00 | $22.50 |

### Cost-Saving Options
- **Prompt caching**: Up to 90% savings
  - Cache reads: $0.30/MTok (Sonnet 4.5)
  - 5-min cache writes: 1.25x base price
  - 1-hour cache writes: 2x base price
- **Batch API**: 50% discount on all tokens

### Rate Limits by Tier

| Tier | Deposit | Spend Limit | RPM | Notes |
|------|---------|-------------|-----|-------|
| Tier 1 | $5 | $100/month | 50 | Entry level |
| Tier 2 | $40 | $500/month | 1,000 | |
| Tier 3 | $200 | $1,000/month | 2,000 | |
| Tier 4 | $400+ | $5,000/month | 4,000 | Maximum self-service |

### Subscription Plans
| Plan | Price | Features |
|------|-------|----------|
| Free | $0 | Limited messages (~5 per day) |
| Pro | $20/month | ~45 messages/5 hours |
| Max | $100/month | 5x Pro usage |
| Max+ | $200/month | 20x Pro usage |
| Team Standard | $25/seat/month | Collaboration features |
| Team Premium | $125/seat/month | 6.25x Pro + Claude Code |

**Sources**: [Anthropic Pricing](https://platform.claude.com/docs/en/about-claude/pricing), [Claude Rate Limits](https://platform.claude.com/docs/en/api/rate-limits)

---

## Google Gemini

### Models & Pricing

| Model | Input $/MTok | Output $/MTok | Context | Notes |
|-------|-------------|---------------|---------|-------|
| **Gemini 3 Pro Preview** | $2.00 (≤200K) / $4.00 (>200K) | $12.00 / $18.00 | ~1M | Latest flagship |
| **Gemini 3 Flash Preview** | $0.50 (text) / $1.00 (audio) | $3.00 | ~1M | Fast |
| **Gemini 2.5 Pro** | $1.25 (≤200K) / $2.50 (>200K) | $10.00 / $15.00 | 1M | Production ready |
| **Gemini 2.5 Flash** | $0.30 | $2.50 | 1M | Best value |
| **Gemini 2.5 Flash-Lite** | $0.10 | $0.40 | 1M | Ultra budget |
| **Gemini 2.0 Flash** | $0.10 | $0.40 | 1M | Being retired Mar 2026 |
| **Gemini 2.0 Flash-Lite** | $0.075 | $0.30 | 1M | Being retired Mar 2026 |

### Cost-Saving Options
- **Batch API**: 50% discount
- **Context caching**: Available for paid tiers

### Rate Limits by Tier

| Tier | Requirements | RPM | RPD | Notes |
|------|-------------|-----|-----|-------|
| Free | None | 5-15 | 20 | No payment required! |
| Tier 1 | Billing enabled | 150-300 | Higher | Instant upgrade |
| Tier 2 | $250 spend + 30 days | Higher | Higher | 24-48h upgrade |
| Tier 3 | $1,000 spend + 30 days | 4,000+ | Custom | Enterprise |

### Key Advantages
- **Free tier without credit card** (unlike OpenAI/Anthropic)
- **1M token context window** standard
- Generous free tier for prototyping

### Image Generation (Imagen)
- 1K-2K resolution: $0.134/image
- 4K resolution: $0.24/image

**Sources**: [Gemini Pricing](https://ai.google.dev/gemini-api/docs/pricing), [Gemini Rate Limits](https://ai.google.dev/gemini-api/docs/rate-limits)

---

## Mistral AI

### Models & Pricing

| Model | Input $/MTok | Output $/MTok | Context | Notes |
|-------|-------------|---------------|---------|-------|
| **Mistral Large 3** | $0.50 | $1.50 | 262K | Latest flagship |
| **Mistral Large 2411** | $2.00 | $6.00 | 131K | Previous flagship |
| **Mistral Medium 3.1** | $0.40 | $2.00 | 131K | Balanced |
| **Mistral Medium 3** | $0.40 | $2.00 | 131K | |
| **Mistral Small 3.2** | $0.06 | $0.18 | 131K | Budget friendly |
| **Mistral Small 3.1** | $0.03 | $0.11 | 131K | Very cheap |
| **Mistral Nemo** | $0.02 | $0.04 | 131K | **Cheapest option** |
| **Codestral** | $0.30 | $0.90 | 256K | Code specialized |
| **Devstral 2** | $0.05 | $0.22 | 262K | Agentic coding |

### Rate Limits
- Free tier available with restrictive limits (prototyping only)
- Limits increase with cumulative billing thresholds
- Contact support for custom enterprise limits
- Check your limits: https://admin.mistral.ai/plateforme/limits

### Key Advantages
- **Very competitive pricing** especially for smaller models
- **Large context windows** (up to 262K)
- Open-weight models available
- Strong code generation (Codestral)

**Sources**: [Mistral Pricing](https://mistral.ai/pricing), [Mistral Rate Limits](https://docs.mistral.ai/deployment/ai-studio/tier)

---

## Comparison: Best For Each Use Case

### Budget-Conscious Development
| Rank | Model | Input $/MTok | Output $/MTok | Total for 1M in + 100K out |
|------|-------|-------------|---------------|---------------------------|
| 1 | Mistral Nemo | $0.02 | $0.04 | $0.024 |
| 2 | Gemini 2.0 Flash-Lite | $0.075 | $0.30 | $0.105 |
| 3 | Gemini 2.5 Flash-Lite | $0.10 | $0.40 | $0.14 |
| 4 | GPT-4o-mini | $0.15 | $0.60 | $0.21 |
| 5 | Claude Haiku 3 | $0.25 | $1.25 | $0.375 |

### Maximum Capability (Flagship Models)
| Model | Input $/MTok | Output $/MTok | Best For |
|-------|-------------|---------------|----------|
| Claude Opus 4.6 | $5.00 | $25.00 | Complex reasoning, coding |
| GPT-4o | $2.50 | $10.00 | General purpose, multimodal |
| Gemini 2.5 Pro | $1.25 | $10.00 | Long context, best value flagship |
| Mistral Large 3 | $0.50 | $1.50 | **Cheapest flagship** |

### Best Value (Quality/Price Ratio)
| Model | Input $/MTok | Output $/MTok | Why |
|-------|-------------|---------------|-----|
| **Gemini 2.5 Flash** | $0.30 | $2.50 | 1M context, great performance |
| **Claude Sonnet 4.5** | $3.00 | $15.00 | Near-flagship quality |
| **Mistral Medium 3.1** | $0.40 | $2.00 | Excellent for most tasks |
| **GPT-4o-mini** | $0.15 | $0.60 | Very capable for price |

### Long Context (1M+ tokens)
| Model | Context | Notes |
|-------|---------|-------|
| Gemini 2.5 Pro/Flash | 1M | Standard, no extra cost |
| Claude Opus/Sonnet | 1M (beta) | Tier 4 required, 2x price >200K |

### Code Generation
| Model | Price Range | Notes |
|-------|-------------|-------|
| Codestral | $0.30-$0.90 | Specialized for code |
| Devstral 2 | $0.05-$0.22 | Agentic coding |
| Claude Sonnet 4.5 | $3.00-$15.00 | Excellent coding abilities |

---

## Recommendations by Use Case

### For Prototyping/Development
**Best choice**: Gemini 2.5 Flash or Mistral Small 3.1
- Reason: Cheap, no credit card needed for Gemini free tier

### For Production (Cost-Sensitive)
**Best choice**: GPT-4o-mini or Gemini 2.5 Flash
- Reason: Good quality at very low cost

### For Production (Quality-First)
**Best choice**: Claude Sonnet 4.5 or Gemini 2.5 Pro
- Reason: Near-flagship performance, reasonable cost

### For Maximum Quality
**Best choice**: Claude Opus 4.6 or GPT-4o
- Reason: Best reasoning and complex task handling

### For Long Documents/RAG
**Best choice**: Gemini 2.5 Pro (1M context standard)
- Reason: Native 1M context without premium pricing

### For Coding Tasks
**Best choice**: Claude Sonnet 4.5 or Codestral
- Reason: Excellent code generation and understanding

---

## Cost Calculator Example

**Processing 10,000 documents (avg 5,000 tokens each, 500 token output)**

| Model | Input Cost | Output Cost | **Total** |
|-------|-----------|-------------|-----------|
| Mistral Nemo | $1.00 | $0.20 | **$1.20** |
| Gemini 2.5 Flash-Lite | $5.00 | $2.00 | **$7.00** |
| GPT-4o-mini | $7.50 | $3.00 | **$10.50** |
| Claude Haiku 4.5 | $50.00 | $25.00 | **$75.00** |
| Claude Sonnet 4.5 | $150.00 | $75.00 | **$225.00** |
| GPT-4o | $125.00 | $50.00 | **$175.00** |
| Claude Opus 4.5 | $250.00 | $125.00 | **$375.00** |

---

## Additional Resources

- [OpenAI Pricing](https://platform.openai.com/docs/pricing)
- [Anthropic Pricing](https://platform.claude.com/docs/en/about-claude/pricing)
- [Google Gemini Pricing](https://ai.google.dev/gemini-api/docs/pricing)
- [Mistral Pricing](https://mistral.ai/pricing)
- [Price Per Token Comparison](https://pricepertoken.com)
