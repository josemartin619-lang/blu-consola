/* Consola Operativa · Blu — Supabase runtime.
   Provides the same window.claude.use('db' | 'user' | 'assets' | 'downloads') interface the
   console was written against, backed by Supabase. Starts the app once the session is known. */
(function(){
'use strict';
var SB_URL = 'https://iodljeikkzwiozbslmku.supabase.co';
var SB_KEY = 'sb_publishable_s8HXMYPEyrjMnexrhRA0JA_vldoiYk3'; // public by design; access is enforced by row-level security
var sb = window.supabase.createClient(SB_URL, SB_KEY, {
  auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true, flowType: 'implicit' }
});
window.__sb = sb;
var S = { session: null, role: null, me: null, booted: false };

function $(s){ return document.querySelector(s); }
function toErr(e){ var x = new Error((e && e.message) || 'error'); x.code = (e && (e.code || e.status)) || 'error'; return x; }
function clone(o){ return JSON.parse(JSON.stringify(o)); }

/* ---------- db: document collections over the `records` table ---------- */
var DB = (function(){
  var cache = {}, loading = {}, pend = {}, subs = {}, chan = null, wasDown = false;
  function snap(col){ var m = cache[col] || {}; return { docs: Object.keys(m).map(function(id){ return { id: id, data: function(){ return clone(m[id]); } }; }) }; }
  function emit(col){ (subs[col] || []).forEach(function(s){ try { s.cb(snap(col)); } catch (e) { console.warn(e); } }); }
  function fetchCol(col){
    var out = {}, page = 0, size = 1000;
    function next(){
      return sb.from('records').select('id,data').eq('col', col).order('id').range(page * size, page * size + size - 1).then(function(r){
        if (r.error) throw toErr(r.error);
        r.data.forEach(function(x){ out[x.id] = x.data; });
        if (r.data.length === size){ page++; return next(); }
        return out;
      });
    }
    return next();
  }
  function apply(col, ev){ if (ev.del) delete cache[col][ev.id]; else cache[col][ev.id] = ev.data; }
  function load(col){
    if (loading[col]) return loading[col];
    pend[col] = [];
    loading[col] = fetchCol(col).then(function(m){
      cache[col] = m; (pend[col] || []).forEach(function(ev){ apply(col, ev); }); delete pend[col]; delete loading[col];
    }, function(e){ delete pend[col]; delete loading[col]; throw e; });
    return loading[col];
  }
  function refetchAll(){ Object.keys(subs).forEach(function(col){ if ((subs[col] || []).length) load(col).then(function(){ emit(col); }).catch(function(){}); }); }
  function ensureChannel(){
    if (chan) return;
    chan = sb.channel('records-live').on('postgres_changes', { event: '*', schema: 'public', table: 'records' }, function(p){
      var del = p.eventType === 'DELETE', row = del ? p.old : p.new;
      if (!row || !row.col) return;
      var ev = { id: row.id, data: row.data, del: del };
      if (pend[row.col]) { pend[row.col].push(ev); return; }
      if (!cache[row.col]) return;
      apply(row.col, ev); emit(row.col);
    }).subscribe(function(status){
      if (status === 'SUBSCRIBED'){ if (wasDown){ wasDown = false; refetchAll(); } }
      else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT' || status === 'CLOSED'){ wasDown = true; }
    });
  }
  var hiddenAt = 0;
  document.addEventListener('visibilitychange', function(){
    if (document.hidden) hiddenAt = Date.now();
    else if (hiddenAt && Date.now() - hiddenAt > 120000) refetchAll();
  });
  return {
    collection: function(col){
      return {
        onSnapshot: function(cb, onErr){
          ensureChannel();
          var s = { cb: cb, err: onErr };
          (subs[col] = subs[col] || []).push(s);
          (cache[col] && !loading[col] ? Promise.resolve() : load(col)).then(function(){
            if ((subs[col] || []).indexOf(s) >= 0) cb(snap(col));
          }).catch(function(e){ if (onErr) onErr(toErr(e)); });
          return function(){ subs[col] = (subs[col] || []).filter(function(x){ return x !== s; }); };
        },
        get: function(){ return fetchCol(col).then(function(m){ return { docs: Object.keys(m).map(function(id){ return { id: id, data: function(){ return clone(m[id]); } }; }) }; }); },
        doc: function(id){
          return {
            set: function(data){
              var had = cache[col] && Object.prototype.hasOwnProperty.call(cache[col], id), prev = had ? cache[col][id] : undefined;
              if (cache[col]){ cache[col][id] = clone(data); emit(col); }
              return sb.from('records').upsert({ col: col, id: id, data: data }).then(function(r){
                if (r.error){ if (cache[col]){ if (had) cache[col][id] = prev; else delete cache[col][id]; emit(col); } throw toErr(r.error); }
              });
            },
            delete: function(){
              var had = cache[col] && Object.prototype.hasOwnProperty.call(cache[col], id), prev = had ? cache[col][id] : undefined;
              if (had){ delete cache[col][id]; emit(col); }
              return sb.from('records').delete().eq('col', col).eq('id', id).then(function(r){
                if (r.error){ if (had){ cache[col][id] = prev; emit(col); } throw toErr(r.error); }
              });
            }
          };
        }
      };
    }
  };
})();

/* ---------- user ---------- */
var USER = {
  canEdit: function(){ return Promise.resolve(S.role === 'team'); },
  name: function(){ return Promise.resolve((S.me && S.me.nombre) || (S.session ? S.session.user.email.split('@')[0] : '')); },
  id: function(){ return Promise.resolve(S.me ? (S.me.legacy_id || S.me.email) : (S.session ? S.session.user.email : null)); },
  profiles: function(ids){
    return sb.from('team_members').select('email,nombre,legacy_id').then(function(r){
      if (r.error) throw toErr(r.error);
      var o = {}; r.data.forEach(function(t){ o[t.legacy_id || t.email] = { name: t.nombre || t.email }; });
      return o;
    });
  }
};

/* ---------- files (private bucket, team only) ---------- */
var ASSETS = {
  upload: function(f, opts){
    var type = (opts && opts.type) || f.type || 'application/octet-stream';
    var name = String(f.name || (type === 'application/json' ? 'respaldo.json' : 'archivo')).replace(/[^\w.\-]+/g, '_').slice(-80);
    var path = new Date().toISOString().slice(0, 10) + '/' + Math.random().toString(36).slice(2, 10) + '-' + name;
    return sb.storage.from('files').upload(path, f, { contentType: type, upsert: false }).then(function(r){
      if (r.error) throw toErr(r.error);
      return { id: path, url: '', sizeBytes: f.size, contentType: type };
    });
  }
};
window.__openFile = function(id){
  var w = window.open('', '_blank');
  sb.storage.from('files').createSignedUrl(id, 300).then(function(r){
    if (r.error){ if (w) w.close(); window.alert('No se pudo abrir el archivo.'); return; }
    if (w) w.location = r.data.signedUrl; else location.href = r.data.signedUrl;
  });
};
window.__readFile = function(id){
  return sb.storage.from('files').download(id).then(function(r){ if (r.error) throw toErr(r.error); return r.data.text(); });
};

/* ---------- downloads ---------- */
var DOWNLOADS = {
  save: function(o){
    var b = o.data instanceof Blob ? o.data : new Blob([o.data]);
    var u = URL.createObjectURL(b), a = document.createElement('a');
    a.href = u; a.download = o.filename || 'archivo'; document.body.appendChild(a); a.click();
    setTimeout(function(){ URL.revokeObjectURL(u); a.remove(); }, 2000);
    return Promise.resolve();
  }
};

var ready;
window.claude = {
  use: function(name){
    return ready.then(function(){
      if (name === 'downloads') return S.session ? DOWNLOADS : null;
      if (S.role !== 'team') return null;
      return { db: DB, user: USER, assets: ASSETS }[name] || null;
    });
  }
};

/* ---------- session and role ---------- */
var hashErr = /error_description=([^&]+)/.exec(location.hash || '');
ready = sb.auth.getSession().then(function(r){
  S.session = r.data && r.data.session;
  if (!S.session) return;
  return sb.rpc('is_team').then(function(t){
    if (t.error) throw t.error;
    if (t.data){
      S.role = 'team';
      return sb.from('team_members').select('email,nombre,legacy_id').eq('email', S.session.user.email.toLowerCase()).maybeSingle().then(function(m){ S.me = m.data || null; });
    }
    return sb.rpc('portal_data').then(function(p){
      if (p.error){ S.role = 'none'; return; }
      S.role = 'client'; window.PORTAL_DATA = p.data;
    });
  });
}).catch(function(e){ console.warn(e); S.error = e; });

sb.auth.onAuthStateChange(function(ev){
  if (ev === 'SIGNED_IN' && S.booted && !S.session) location.replace(location.pathname);
  if (ev === 'SIGNED_OUT' && S.booted && S.session) location.replace(location.pathname);
});

function msg(lines, kind){
  var m = $('#loginMsg'); if (!m) return;
  m.innerHTML = '';
  if (!lines){ m.hidden = true; return; }
  lines.forEach(function(n){ if (typeof n === 'string'){ var p = document.createElement('p'); p.textContent = n; m.appendChild(p); } else m.appendChild(n); });
  m.classList.toggle('bad', kind === 'bad');
  m.hidden = false;
}
function signOut(){ sb.auth.signOut().then(function(){ location.replace(location.pathname); }); }
window.__signOut = signOut;

function wireAuthForms(){
  var box = $('#authBox'), code = $('#codeBox'), email = $('#authEmail'), tok = $('#authCode'), sentTo = '';
  box.hidden = false;
  setTimeout(function(){ try { email.focus({ preventScroll: true }); } catch (e) {} }, 400);
  box.addEventListener('submit', function(e){
    e.preventDefault();
    var v = String(email.value || '').trim().toLowerCase();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(v)){ msg(['Escriba un correo válido.'], 'bad'); return; }
    var btn = box.querySelector('button[type=submit]'); btn.disabled = true; msg(['Verificando…']);
    sb.rpc('can_login', { p_email: v }).then(function(r){
      if (r.error) throw r.error;
      if (!r.data){ btn.disabled = false; msg(['Este correo no tiene acceso.', 'Si trabaja con Blu, pídanos que lo agreguemos.'], 'bad'); return; }
      return sb.auth.signInWithOtp({ email: v, options: { shouldCreateUser: true, emailRedirectTo: location.origin + location.pathname } }).then(function(s){
        btn.disabled = false;
        if (s.error){
          var rl = /rate|limit|security purposes/i.test(s.error.message || '');
          msg([rl ? 'Se enviaron demasiados correos. Espere un momento e inténtelo de nuevo.' : 'No se pudo enviar el correo. Inténtelo de nuevo.'], 'bad'); return;
        }
        sentTo = v; box.hidden = true; code.hidden = false; msg(['Le enviamos un correo a ' + v + '.', 'Abra el enlace del correo. Si trae un código, escríbalo aquí.']);
        setTimeout(function(){ try { tok.focus(); } catch (e) {} }, 50);
      });
    }).catch(function(){ btn.disabled = false; msg(['No hay conexión con el servidor. Inténtelo de nuevo.'], 'bad'); });
  });
  code.addEventListener('submit', function(e){
    e.preventDefault();
    var t = String(tok.value || '').replace(/\D/g, '');
    if (t.length < 6){ msg(['Escriba el código completo del correo.'], 'bad'); return; }
    var btn = code.querySelector('button[type=submit]'); btn.disabled = true; msg(['Verificando…']);
    sb.auth.verifyOtp({ email: sentTo, token: t, type: 'email' }).then(function(r){
      if (r.error){ btn.disabled = false; msg(['Ese código no es válido o ya venció.'], 'bad'); return; }
      location.replace(location.pathname);
    });
  });
  $('#authBack').addEventListener('click', function(){ code.hidden = true; box.hidden = false; tok.value = ''; msg(null); try { email.focus(); } catch (e) {} });
}

function afterStart(){
  var doors = $('#doors'), foot = $('.login-foot');
  if (!S.session){
    if (doors) doors.hidden = true;
    if (S.error) msg(['No hay conexión con el servidor. Recargue la página.'], 'bad');
    else if (hashErr) msg(['El enlace ya venció o ya se usó. Pida uno nuevo.'], 'bad');
    wireAuthForms();
    return;
  }
  var who = document.createElement('span'); who.className = 'who';
  who.appendChild(document.createTextNode(S.session.user.email + ' · '));
  var out = document.createElement('button'); out.type = 'button'; out.className = 'linkish'; out.textContent = 'Cerrar sesión'; out.addEventListener('click', signOut);
  who.appendChild(out);
  if (foot){ foot.textContent = ''; foot.appendChild(who); }
  if (S.role === 'none' || !S.role){
    if (doors) doors.hidden = true;
    msg([S.error ? 'No hay conexión con el servidor. Recargue la página.' : 'Este correo todavía no tiene acceso a la consola ni a un portal.', 'Pídanos que lo agreguemos.'], 'bad');
    return;
  }
  if (S.role === 'client'){ var bk = $('#backupBtn'); if (bk) bk.remove(); }
  var fb = $('#fontBtn');
  if (fb){
    var b = document.createElement('button'); b.className = 'nav-btn'; b.type = 'button'; b.id = 'signOutBtn';
    b.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M15 4h3a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2h-3M10 17l5-5-5-5M15 12H4"/></svg><span>Cerrar sesión</span>';
    b.addEventListener('click', signOut);
    fb.parentNode.insertBefore(b, fb.nextSibling);
    if (S.role === 'team'){
      var a = document.createElement('button'); a.className = 'nav-btn'; a.type = 'button'; a.id = 'accessBtn';
      a.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="9" cy="8" r="3.2"/><path d="M3.5 19c.8-3.2 3-5 5.5-5s4.7 1.8 5.5 5M16 6.5a3 3 0 0 1 0 5.6M18 14.5c1.4.7 2.3 2.2 2.6 4.5"/></svg><span>Accesos</span>';
      a.addEventListener('click', openAccess);
      fb.parentNode.insertBefore(a, b);
    }
  }
}

/* ---------- Accesos: who can sign in (team only) ---------- */
function openAccess(){
  var U = window.__ui; if (!U) return;
  var h = U.h, body = h('div', { style: 'display:grid;gap:22px' });
  function field(label, input){ return h('div', { class: 'field' }, h('label', { text: label }), input); }
  function rowOf(main, sub, onRemove){
    return h('div', { class: 'row', style: 'grid-template-columns:minmax(0,1fr) auto' },
      h('span', { style: 'display:grid;gap:2px;min-width:0' }, h('span', { class: 't', text: main }), sub ? h('span', { class: 's', text: sub }) : null),
      onRemove ? h('button', { class: 'btn small ghost danger', type: 'button', text: 'Quitar', onclick: onRemove }) : h('span', {}));
  }
  function fail(e){ U.toast(e && e.code === '23505' ? 'Ese correo ya tiene acceso' : 'No se pudo guardar el acceso'); }
  var EMAIL = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
  function draw(){
    Promise.all([sb.from('team_members').select('email,nombre').order('email'), sb.from('client_users').select('email,client_id,nombre').order('email')]).then(function(r){
      var team = r[0].data || [], cu = r[1].data || [], clients = U.clients(), names = {};
      clients.forEach(function(c){ names[c.id] = c.nombre; });
      body.innerHTML = '';
      body.appendChild(h('p', { class: 'muted', style: 'margin:0;line-height:1.55', text: 'Quién puede entrar y qué ve. Cada persona entra con su correo; no hay contraseñas.' }));

      var tl = h('div', { class: 'list' });
      team.forEach(function(t){
        var self = S.session && t.email === S.session.user.email.toLowerCase();
        tl.appendChild(rowOf((t.nombre ? t.nombre + ' · ' : '') + t.email + (self ? ' (usted)' : ''), null, self ? null : function(){
          sb.from('team_members').delete().eq('email', t.email).then(function(x){ if (x.error) fail(x.error); else { U.toast('Acceso quitado'); draw(); } });
        }));
      });
      var te = h('input', { type: 'email', placeholder: 'socio@agenciablu.com' }), tn = h('input', { type: 'text', placeholder: 'Nombre' });
      var tf = h('form', { style: 'display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1fr) auto;gap:8px;align-items:end' },
        field('Correo', te), field('Nombre', tn), h('button', { class: 'btn small primary', type: 'submit', text: 'Agregar' }));
      tf.addEventListener('submit', function(e){ e.preventDefault(); var v = te.value.trim().toLowerCase(); if (!EMAIL.test(v)) return;
        sb.from('team_members').insert({ email: v, nombre: tn.value.trim() || null }).then(function(x){ if (x.error) fail(x.error); else { U.toast('Acceso agregado'); draw(); } }); });
      body.appendChild(h('section', { style: 'display:grid;gap:10px' }, h('div', { class: 'h2', text: 'Equipo' }), h('div', { class: 'muted', style: 'font-size:13px', text: 'Ve todo y puede editar.' }), tl, tf));

      var cl = h('div', { class: 'list' });
      if (!cu.length) cl.appendChild(h('div', { class: 'row', style: 'grid-template-columns:1fr' }, h('span', { class: 's', text: 'Todavía no hay accesos de clientes.' })));
      cu.forEach(function(c){
        cl.appendChild(rowOf((c.nombre ? c.nombre + ' · ' : '') + c.email, names[c.client_id] || c.client_id, function(){
          sb.from('client_users').delete().eq('email', c.email).then(function(x){ if (x.error) fail(x.error); else { U.toast('Acceso quitado'); draw(); } });
        }));
      });
      var ce = h('input', { type: 'email', placeholder: 'gerente@cliente.com' }), cs = h('select', {});
      clients.sort(function(a, b){ return (a.nombre || '').localeCompare(b.nombre || ''); }).forEach(function(c){ cs.appendChild(h('option', { value: c.id, text: c.nombre })); });
      var cf = h('form', { style: 'display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1fr) auto;gap:8px;align-items:end' },
        field('Correo', ce), field('Cliente', cs), h('button', { class: 'btn small primary', type: 'submit', text: 'Agregar' }));
      cf.addEventListener('submit', function(e){ e.preventDefault(); var v = ce.value.trim().toLowerCase(); if (!EMAIL.test(v) || !cs.value) return;
        sb.from('client_users').insert({ email: v, client_id: cs.value }).then(function(x){ if (x.error) fail(x.error); else { U.toast('Acceso agregado'); draw(); } }); });
      body.appendChild(h('section', { style: 'display:grid;gap:10px' }, h('div', { class: 'h2', text: 'Portales de clientes' }), h('div', { class: 'muted', style: 'font-size:13px', text: 'Ve solo el portal de su empresa, sin notas internas.' }), cl, cf,
        h('div', { class: 'muted', style: 'font-size:12.5px', text: 'Para entrar: consola.agenciablu.com con ese correo.' })));
    });
  }
  draw(); U.openDrawer('Accesos', body);
}

/* The console itself lives in private storage: only signed-in team members and clients can download it. */
var APP_FILE = 'app-d386a8c6ee.js';
function loadApp(){
  return sb.storage.from('app').download(APP_FILE).then(function(r){
    if (r.error) throw toErr(r.error);
    return r.data.text();
  }).then(function(code){
    var sc = document.createElement('script'); sc.textContent = code + '\n//# sourceURL=consola-app.js';
    document.body.appendChild(sc);
    if (typeof window.__startApp !== 'function') throw new Error('app did not load');
  });
}

ready.then(function(){
  if (location.hash && /access_token|error/.test(location.hash)) history.replaceState(null, '', location.pathname + location.search);
  S.booted = true;
  var needsApp = S.role === 'team' || S.role === 'client';
  return (needsApp ? loadApp() : Promise.resolve()).then(function(){
    if (needsApp) window.__startApp();
    afterStart();
  }, function(e){
    console.warn(e); S.error = e; S.role = 'none';
    afterStart();
  });
});
})();
