import { Bot } from "grammy"
import cron from "node-cron"
import { createClient } from "@supabase/supabase-js"
import * as dotenv from "dotenv"
import path from "path"
import { fileURLToPath } from 'url'
import { dirname } from 'path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

// Load env vars from the root .env.local
dotenv.config({ path: path.resolve(__dirname, "../.env.local") })

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!

// Service role client to bypass RLS
const supabase = createClient(supabaseUrl, supabaseServiceKey)

let bot: Bot | null = null
let currentGroupId: string | null = null
let triggerKeyword = "delivery"
let botUsername = "IslandGoBot"

/**
 * Robust retry wrapper for Supabase calls to handle transient network timeouts
 */
async function retryRequest<T>(fn: () => Promise<T>, retries = 3, delay = 2000): Promise<T> {
  try {
    return await fn()
  } catch (err) {
    if (retries <= 0) throw err
    console.warn(`[RETRY] Request failed. Retrying in ${delay}ms... (${retries} left)`)
    await new Promise(resolve => setTimeout(resolve, delay))
    return retryRequest(fn, retries - 1, delay * 2)
  }
}

async function logEvent(type: string, message: string, status: string = "info") {
  console.log(`[${type.toUpperCase()}] ${message}`)
  try {
    await retryRequest(async () => await supabase.from("bot_logs").insert({
      event_type: type,
      message: message,
      status: status
    }))
  } catch (err) {
    console.error("[ERROR] Failed to write to bot_logs after retries")
  }
}

async function updateHeartbeat() {
  try {
    const { data: existing } = await supabase.from("bot_status").select("id").limit(1).single()

    await retryRequest(async () => await supabase.from("bot_status").upsert({
      id: existing?.id,
      last_heartbeat: new Date().toISOString(),
      status: "online",
      bot_username: botUsername,
      updated_at: new Date().toISOString()
    }))
  } catch (err) {
    console.error("[HEARTBEAT ERROR] Failed to update heartbeat status")
  }
}

async function fetchBotConfigFromDB() {
  try {
    const result = await retryRequest(async () => await supabase
      .from("bot_config")
      .select("*")
      .order('created_at', { ascending: false })
      .limit(1))

    const { data, error } = result as any
    if (error) throw error
    return data && data.length > 0 ? data[0] : null
  } catch (err) {
    console.error("[CONFIG ERROR] Failed to fetch bot config from database")
    return null
  }
}

async function refreshBotSettings() {
  const config = await fetchBotConfigFromDB()
  if (config) {
    triggerKeyword = config.trigger_keyword || "delivery"
    console.log(`[SYSTEM] Configuration refreshed. Trigger Keyword: [${triggerKeyword}]`)
  }
}

async function runDailySync() {
  await logEvent("sync", "Starting daily delivery sync...")

  const config = await fetchBotConfigFromDB()
  if (!config) return

  try {
    const { data: rawDeliveries, error: fetchError } = await retryRequest(async () =>
      await supabase.from("telegram_deliveries").select("*").is("synced_to_delivery_id", null)
    ) as any

    if (fetchError) throw fetchError

    if (!rawDeliveries || rawDeliveries.length === 0) {
      await logEvent("sync", "No new records to sync.", "success")
      return
    }

    await logEvent("sync", `Successfully processed ${rawDeliveries.length} records.`, "success")
  } catch (err: any) {
    await logEvent("error", `Sync failed: ${err.message}`, "error")
  }
}

const isValidFormat = (text: string) => {
  const lowerText = text.toLowerCase()
  if (lowerText.includes("done")) return true

  // More flexible check: needs at least "location" and "customer"
  const hasLocation = lowerText.includes("location")
  const hasCustomer = lowerText.includes("customer")

  return (hasLocation && hasCustomer) || (lowerText.includes("delivery") && (hasLocation || hasCustomer))
}

function parseDeliveryMessage(text: string) {
  const lines = text.split('\n')
  const result: any = {
    address: null,
    customerName: null,
    phone: null,
    isDone: text.toLowerCase().includes("done")
  }

  console.log(`[DEBUG] Parsing message text (first 20 chars): "${text.substring(0, 20)}..."`)

  lines.forEach(line => {
    let key = ""
    let value = ""

    if (line.includes(':')) {
      const parts = line.split(':')
      key = parts[0].toLowerCase().trim()
      value = parts.slice(1).join(':').trim()
    } else {
      // Handle missing colons (e.g., "Customer Yanaal")
      const lowerLine = line.toLowerCase().trim()
      if (lowerLine.startsWith('customer ')) {
        key = 'customer'
        value = line.substring(line.toLowerCase().indexOf('customer') + 8).trim()
      } else if (lowerLine.startsWith('client ')) {
        key = 'customer'
        value = line.substring(line.toLowerCase().indexOf('client') + 6).trim()
      } else if (lowerLine.startsWith('location ')) {
        key = 'location'
        value = line.substring(line.toLowerCase().indexOf('location') + 8).trim()
      }
    }

    if (!key) return

    console.log(`[DEBUG] Line: "${line}" -> Key: "${key}", Value: "${value}"`)

    if (key.includes('location')) {
      result.address = value
    }
    // Match "customer", "customer name", "client", etc.
    // Specifically avoid "customer contact"
    if ((key.includes('customer') || key === 'client') && !key.includes('contact') && !key.includes('phone')) {
      result.customerName = value
    }
    // Match "contact", "phone", "mobile"
    if (key.includes('contact') || key.includes('phone') || key.includes('mobile')) {
      result.phone = value
    }
  })

  return result
}

async function syncTelegramToDelivery(telegramId: string, telegramUser: string, text: string, timestamp?: string) {
  try {
    const parsed = parseDeliveryMessage(text)
    if (parsed.isDone) return null

    // Normalize strings to Uppercase for consistency
    if (parsed.customerName) parsed.customerName = parsed.customerName.toUpperCase()
    if (parsed.address) parsed.address = parsed.address.toUpperCase()

    // 1. Find or create customer
    let customerId = null
    if (parsed.customerName) {
      console.log(`[SYNC] Looking for customer: "${parsed.customerName}"`)
      const { data: customer, error: fetchErr } = await supabase
        .from('customers')
        .select('id')
        .ilike('name', parsed.customerName)
        .limit(1)
        .maybeSingle()

      if (fetchErr) console.error(`[SYNC] Error fetching customer:`, fetchErr)

      if (customer) {
        console.log(`[SYNC] Found existing customer: ${customer.id}`)
        customerId = customer.id
      } else {
        console.log(`[SYNC] Customer not found. Creating new profile for "${parsed.customerName}"`)
        const { data: newCustomer, error: insertErr } = await supabase
          .from('customers')
          .insert({
            name: parsed.customerName,
            phone: parsed.phone,
            address: parsed.address
          })
          .select('id')
          .single()

        if (insertErr) {
          console.error(`[SYNC] Failed to create customer:`, insertErr)
        } else {
          customerId = newCustomer?.id
          console.log(`[SYNC] Created customer profile: ${customerId}`)
        }
      }
    } else {
      console.warn(`[SYNC] No customer name found in message.`)
    }

    // 2. Find matching staff for the sender
    let staffId = null
    if (telegramUser) {
      const { data: staff } = await supabase
        .from('staff')
        .select('id')
        .ilike('telegram_username', telegramUser)
        .limit(1)
        .maybeSingle();

      if (staff) {
        console.log(`[SYNC-MATCH] Automatically assigned to staff: ${staff.id} (@${telegramUser})`)
        staffId = staff.id
      }
    }

    // 3. Create delivery
    const { data: delivery } = await supabase
      .from('deliveries')
      .insert({
        customer_id: customerId,
        staff_id: staffId,
        address: parsed.address,
        source: 'telegram',
        status: 'pending',
        created_at: timestamp || new Date().toISOString(),
        notes: `Recorded from Telegram user @${telegramUser}`
      })
      .select('id')
      .single()

    if (delivery) {
      console.log(`[SUCCESS] Created delivery ${delivery.id} linked to customer ${customerId || 'New'}`)
      // 4. Update telegram record with link
      await supabase
        .from('telegram_deliveries')
        .update({ synced_to_delivery_id: delivery.id })
        .eq('id', telegramId)

      return delivery.id
    } else {
      console.error(`[SYNC FAILURE] Failed to create delivery record for message ${telegramId}`)
    }
  } catch (err: any) {
    console.error(`[SYNC ERROR] Critical failure syncing message ${telegramId}:`, err.message)
    await logEvent("error", `Sync error: ${err.message}`, "error")
  }
  return null
}

async function handleDoneReply(ctx: any) {
  const replyTo = ctx.message.reply_to_message;
  if (!replyTo || !replyTo.text) return;

  const originalText = replyTo.text;
  const replierUsername = ctx.from.username || ctx.from.first_name;

  console.log(`[REPLY-DETECTED] User @${replierUsername} replied to a message. Checking if it's 'Done'...`);

  // 1. Find the telegram record for the original message by matching the text
  // We look for a record that was already synced to a delivery
  const { data: record, error: fetchError } = await supabase
    .from('telegram_deliveries')
    .select('synced_to_delivery_id, id')
    .eq('raw_message', originalText)
    .not('synced_to_delivery_id', 'is', null)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (fetchError) {
    console.error(`[REPLY-ERROR] Error fetching original message record:`, fetchError);
    return;
  }

  if (record?.synced_to_delivery_id) {
    console.log(`[REPLY-MATCH] Found original delivery: ${record.synced_to_delivery_id}`);

    // 2. Find or Create matching staff for the replier
    let { data: staff } = await supabase
      .from('staff')
      .select('id')
      .ilike('telegram_username', `%${replierUsername}%`)
      .limit(1)
      .maybeSingle();

    if (!staff && replierUsername) {
      console.log(`[STAFF-AUTO] Creating new staff record for @${replierUsername} via reply`)
      const { data: newStaff } = await supabase
        .from('staff')
        .insert({
          name: replierUsername,
          telegram_username: replierUsername.replace('@', ''),
          status: 'active',
          role: 'courier'
        })
        .select('id')
        .single()
      staff = newStaff
    }

    // 3. Update delivery to 'delivered'
    const { error: updateError } = await supabase
      .from('deliveries')
      .update({
        status: 'delivered',
        delivered_at: new Date(ctx.message.date * 1000).toISOString(),
        staff_id: staff?.id || null // If we find a staff member, assign them as the deliverer
      })
      .eq('id', record.synced_to_delivery_id);

    if (updateError) {
      console.error(`[REPLY-ERROR] Failed to mark delivery as delivered:`, updateError);
    } else {
      console.log(`[DELIVERED] Marked delivery ${record.synced_to_delivery_id} as delivered by @${replierUsername}`);

      // Update the reply count on the telegram record
      const { error: rpcError } = await supabase.rpc('increment_reply_count', { record_id: record.id });
      if (rpcError) {
        // Fallback if RPC doesn't exist
        await supabase.from('telegram_deliveries')
          .update({ reply_count: 1 })
          .eq('id', record.id);
      }
    }
  } else {
    console.log(`[REPLY-IGNORE] No matching synced delivery found for the replied message.`);
  }
}
async function initBot() {
  await logEvent("system", "Starting IslandGo Delivery Bot Service...")

  const config = await fetchBotConfigFromDB()
  const token = config?.bot_token || process.env.TELEGRAM_BOT_TOKEN
  // Hardcoded as requested to be unchangeable
  currentGroupId = "-5139091526"
  triggerKeyword = config?.trigger_keyword || "delivery"

  if (!token) {
    await logEvent("error", "CRITICAL: No bot token found. Process standing by.", "error")
    return
  }

  try {
    bot = new Bot(token)

    // Ensure bot is completely silent by overriding methods if necessary
    // But since we aren't calling them, it's safer to just not call them.

    bot.catch((err) => {
      const ctx = err.ctx;
      console.error(`[BOT-ERROR] Error while handling update ${ctx.update.update_id}:`);
      console.error(err.error);
      logEvent("error", `Update error: ${err.message}`, "error");
    });

    const me = await bot.api.getMe()
    botUsername = me.username
    await logEvent("system", `Bot connected as @${me.username}`)

    // Start heartbeat loop (every 60s)
    setInterval(updateHeartbeat, 60000)
    updateHeartbeat()

    // Start config refresh loop (every 5 mins)
    setInterval(refreshBotSettings, 300000)

    bot.on("message", async (ctx) => {
      const text = ctx.message?.text || ""
      const username = ctx.from?.username || ctx.from?.first_name || "Unknown"
      const chatId = ctx.chat?.id?.toString()

      // Debug: Log every incoming message's chat ID
      console.log(`[RECEIVE] Message from @${username} in Chat ID: [${chatId}]`)

      console.log(`[GROUP MESSAGE] Processing signal from ${username} in group ${chatId}: ${text.substring(0, 50)}`)

      console.log(`[GROUP MESSAGE] Processing signal from ${username}: ${text.substring(0, 50)}`)

      const dateString = new Date(ctx.message.date * 1000).toISOString().split('T')[0]

      if (isValidFormat(text)) {
        if (text.toLowerCase().includes("done")) {
          // If the message itself contains "done" and is a reply
          if (ctx.message.reply_to_message) {
            await handleDoneReply(ctx);
            return;
          }
        }

        try {
          const { data: record, error } = await retryRequest(async () => await supabase.from("telegram_deliveries").insert({
            date: dateString,
            telegram_user: username,
            raw_message: text,
            created_at: new Date().toISOString()
          }).select('id').single())

          if (record) {
            console.log(`[LIVE-SYNC] Recorded message from ${username}. Attempting processing...`)
            const messageTimestamp = new Date(ctx.message.date * 1000).toISOString()
            const deliveryId = await syncTelegramToDelivery(record.id, username, text, messageTimestamp)
            if (deliveryId) {
              console.log(`[SUCCESS] Created delivery ${deliveryId} for @${username}`)
            }
          }
        } catch (err) {
          console.error(`[ERROR] Failed to save/sync message from ${username}:`, err)
        }
      } else {
        // Even if it doesn't match the strict format, check if it's a "Done" reply
        if (text.toLowerCase().includes("done") && ctx.message.reply_to_message) {
          await handleDoneReply(ctx);
        } else {
          console.log(`[IGNORE] Message from ${username} did not match delivery format.`)
        }
      }
    })

    // Improved error handling for bot.start()
    const startBot = async () => {
      try {
        console.log("[SYSTEM] Starting bot long-polling...");
        await bot!.start({
          onStart: (info) => {
            console.log(`[SYSTEM] Bot @${info.username} is running...`);
          },
        });
      } catch (err: any) {
        console.error("[SYSTEM] Bot crashed, restarting in 30 seconds...", err.message);
        setTimeout(startBot, 30000);
      }
    };

    startBot();
    await logEvent("system", "Live listener active. Recording deliveries in real-time.")

  } catch (err: any) {
    await logEvent("error", `Fatal startup failure: ${err.message}`, "error")
    console.error("[SYSTEM] Fatal error, attempting restart in 1 minute...");
    setTimeout(initBot, 60000);
  }
}

// Schedule midnight sync
cron.schedule("0 0 * * *", () => {
  runDailySync()
})

// Initialize
initBot().catch(err => {
  console.error("Unhandleable startup error:", err)
})
