import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.0";
import { corsHeaders } from "../_shared/cors.ts";

const GATEWAY_BASE = "https://connector-gateway.lovable.dev";
const S3_BUCKET = "kotobi";
const S3_REGION = "eu-north-1";
const S3_PUBLIC_URL_PREFIX = `https://${S3_BUCKET}.s3.${S3_REGION}.amazonaws.com/`;
const SUPABASE_HOST = "supabase.co";

interface BookRow {
  id: string;
  book_file_url: string;
  file_type: string | null;
}

async function getSignedPutUrl(objectKey: string, lovableKey: string, s3Key: string): Promise<string> {
  const res = await fetch(
    `${GATEWAY_BASE}/api/v1/sign_storage_url?provider=aws_s3&mode=write`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${lovableKey}`,
        "X-Connection-Api-Key": s3Key,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ object_path: objectKey }),
    },
  );
  if (!res.ok) {
    throw new Error(`sign_storage_url failed [${res.status}]: ${await res.text()}`);
  }
  const data = await res.json();
  return data.url as string;
}

function deriveObjectKey(supabaseUrl: string): string {
  // .../storage/v1/object/public/book-files/books/<filename>.pdf
  const marker = "/book-files/";
  const idx = supabaseUrl.indexOf(marker);
  if (idx === -1) {
    // fallback: last path segment under /books/
    const u = new URL(supabaseUrl);
    const parts = u.pathname.split("/");
    const filename = parts[parts.length - 1];
    return `books/${filename}`;
  }
  return supabaseUrl.substring(idx + marker.length);
}

async function migrateOne(
  book: BookRow,
  supabase: ReturnType<typeof createClient>,
  lovableKey: string,
  s3Key: string,
): Promise<{ id: string; ok: boolean; error?: string; new_url?: string }> {
  try {
    const objectKey = deriveObjectKey(book.book_file_url);

    // 1. Download from Supabase Storage (public URL)
    const dl = await fetch(book.book_file_url);
    if (!dl.ok) {
      return { id: book.id, ok: false, error: `download ${dl.status}` };
    }
    const contentType = dl.headers.get("content-type") || "application/pdf";
    const buf = await dl.arrayBuffer();

    // 2. Get signed PUT URL
    const putUrl = await getSignedPutUrl(objectKey, lovableKey, s3Key);

    // 3. Upload to S3
    const up = await fetch(putUrl, {
      method: "PUT",
      headers: { "Content-Type": contentType },
      body: buf,
    });
    if (!up.ok) {
      return { id: book.id, ok: false, error: `upload ${up.status}: ${await up.text()}` };
    }

    // 4. Update DB
    const newUrl = `${S3_PUBLIC_URL_PREFIX}${objectKey}`;
    const { error: updErr } = await supabase
      .from("approved_books")
      .update({ book_file_url: newUrl })
      .eq("id", book.id);
    if (updErr) {
      return { id: book.id, ok: false, error: `db update: ${updErr.message}` };
    }

    return { id: book.id, ok: true, new_url: newUrl };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { id: book.id, ok: false, error: msg };
  }
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }

  try {
    const LOVABLE_API_KEY = Deno.env.get("LOVABLE_API_KEY");
    const AWS_S3_API_KEY = Deno.env.get("AWS_S3_API_KEY");
    const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
    const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

    if (!LOVABLE_API_KEY || !AWS_S3_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
      return new Response(
        JSON.stringify({ error: "Missing required env vars" }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    const body = await req.json().catch(() => ({}));
    const batchSize = Math.max(1, Math.min(20, Number(body.batch_size) || 5));

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // Pick remaining books still on Supabase storage. Order by file_size ASC
    // so small files migrate first (faster progress + less time-out risk).
    const { data: books, error: selErr } = await supabase
      .from("approved_books")
      .select("id, book_file_url, file_type, file_size")
      .ilike("book_file_url", `%${SUPABASE_HOST}%`)
      .order("file_size", { ascending: true, nullsFirst: true })
      .limit(batchSize);

    if (selErr) {
      return new Response(
        JSON.stringify({ error: `select failed: ${selErr.message}` }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    if (!books || books.length === 0) {
      return new Response(
        JSON.stringify({ done: true, message: "No remaining books on Supabase storage" }),
        { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } },
      );
    }

    const results = [];
    for (const b of books as unknown as BookRow[]) {
      const r = await migrateOne(b, supabase, LOVABLE_API_KEY, AWS_S3_API_KEY);
      results.push(r);
      console.log(`book ${b.id}: ${r.ok ? "OK" : "FAIL " + r.error}`);
    }

    const success = results.filter((r) => r.ok).length;
    const failed = results.length - success;

    // Remaining count after this batch
    const { count } = await supabase
      .from("approved_books")
      .select("id", { count: "exact", head: true })
      .ilike("book_file_url", `%${SUPABASE_HOST}%`);

    return new Response(
      JSON.stringify({
        processed: results.length,
        success,
        failed,
        remaining: count ?? null,
        results,
      }),
      { status: 200, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("migrate-books-to-s3 error", msg);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } },
    );
  }
});
