'use strict';

/* ── CFG ─────────────────────────────────────────────────────── */
const CFG = {
  SUPABASE_URL:      'https://bdqfuaknxzcbygtqofrd.supabase.co',
  SUPABASE_ANON_KEY: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJkcWZ1YWtueHpjYnlndHFvZnJkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzMzODgyNjMsImV4cCI6MjA4ODk2NDI2M30.Tkux4CttnhozVriofDWg_VLnKgx3IfUupwZypLw0qr8',
  SUPABASE_VIEW:     'v_match_predictions_live',
  DATA_MODE:         'auto',   // 'auto' | 'supabase' | 'mock'
  CACHE_TTL_MS:      5 * 60 * 1000,
};
