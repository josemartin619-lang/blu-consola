/* Consola Operativa · Blu — Supabase runtime.
   Provides the same window.claude.use('db' | 'user' | 'assets' | 'downloads') interface the
   console was written against, backed by Supabase. Starts the app once the session is known. */
(function(){
'use strict';
var urlType = (/[#&]type=([a-z_]+)/.exec(location.hash || '') || [])[1] || '';
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
  return sb.auth.mfa.getAuthenticatorAssuranceLevel().then(function(a){
    if (a && a.data && a.data.nextLevel === 'aal2' && a.data.currentLevel !== 'aal2'){ S.mfaNeeded = true; return; }
    if (urlType === 'signup' || urlType === 'recovery' || urlType === 'invite'){ S.setPassword = true; return; }
    return loadRole();
  });
}).catch(function(e){ console.warn(e); S.error = e; });
function loadRole(){
  return sb.rpc('is_team').then(function(t){
    if (t.error) throw t.error;
    if (t.data){
      S.role = 'team';
      return sb.from('team_members').select('email,nombre,legacy_id,rol').eq('email', S.session.user.email.toLowerCase()).maybeSingle().then(function(m){ S.me = m.data || null; S.socio = !S.me || S.me.rol !== 'empleado'; window.BLU_ROLE = S.socio ? 'socio' : 'empleado'; });
    }
    return sb.rpc('portal_data').then(function(p){
      if (p.error){ S.role = 'none'; return; }
      S.role = 'client'; window.PORTAL_DATA = p.data;
    });
  });
}

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

var EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
function pwChecks(p, p2){ return { len: p.length >= 10, lower: /[a-z]/.test(p), upper: /[A-Z]/.test(p), digit: /\d/.test(p), same: p.length > 0 && p === p2 }; }
function pwOk(p, p2){ var c = pwChecks(p, p2); return c.len && c.lower && c.upper && c.digit && c.same; }
function randomPassword(){
  var a = new Uint8Array(24); crypto.getRandomValues(a);
  var cs = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789', out = '';
  for (var i = 0; i < a.length; i++) out += cs[a[i] % cs.length];
  return 'Aa1' + out;
}
/* Invitation: creates the account (if new) and emails a single-use link to set a password.
   Existing accounts get a "set a new password" link instead. The invitee never receives a password by email. */
function sendInvite(email){
  var redirect = location.origin + location.pathname;
  return sb.auth.signUp({ email: email, password: randomPassword(), options: { emailRedirectTo: redirect } }).then(function(r){
    if (r.error) throw r.error;
    var u = r.data && r.data.user;
    if (u && u.identities && u.identities.length === 0){
      return sb.auth.resetPasswordForEmail(email, { redirectTo: redirect }).then(function(x){ if (x.error) throw x.error; return 'reset'; });
    }
    return 'invite';
  });
}
window.__sendInvite = sendInvite;
function friendlyAuthError(e){
  var m = (e && e.message) || '';
  if (/invalid login credentials/i.test(m)) return 'Correo o contraseña incorrectos.';
  if (/email not confirmed/i.test(m)) return 'Su acceso todavía no está activado. Abra el enlace del correo de invitación o pida que se lo reenvíen.';
  if (/rate|limit|security purposes/i.test(m)) return 'Demasiados intentos. Espere un momento e inténtelo de nuevo.';
  if (/password/i.test(m) && /short|length|character|weak|requirements/i.test(m)) return 'La contraseña no cumple los requisitos.';
  if (/reauthentication|recent/i.test(m)) return 'Por seguridad, cierre sesión y vuelva a entrar antes de cambiar la contraseña.';
  return 'No se pudo completar. Inténtelo de nuevo.';
}
window.__friendlyAuthError = friendlyAuthError;

function wireAuthForms(){
  var box = $('#authBox'), email = $('#authEmail'), pass = $('#authPass');
  box.hidden = false;
  setTimeout(function(){ try { email.focus({ preventScroll: true }); } catch (e) {} }, 400);
  box.addEventListener('submit', function(e){
    e.preventDefault();
    var v = String(email.value || '').trim().toLowerCase(), p = String(pass.value || '');
    if (!EMAIL_RE.test(v)){ msg(['Escriba un correo válido.'], 'bad'); return; }
    if (!p){ msg(['Escriba su contraseña.'], 'bad'); try { pass.focus(); } catch (x) {} return; }
    var btn = box.querySelector('button[type=submit]'); btn.disabled = true; msg(['Verificando…']);
    sb.rpc('can_login', { p_email: v }).then(function(r){
      if (r.error) throw r.error;
      if (!r.data){ btn.disabled = false; msg(['Este correo no tiene acceso.', 'Si trabaja con Blu, pídanos que lo agreguemos.'], 'bad'); return; }
      return sb.auth.signInWithPassword({ email: v, password: p }).then(function(x){
        if (x.error){ btn.disabled = false; msg([friendlyAuthError(x.error)], 'bad'); return; }
        location.replace(location.pathname);
      });
    }).catch(function(){ btn.disabled = false; msg(['No hay conexión con el servidor. Inténtelo de nuevo.'], 'bad'); });
  });
  $('#authForgot').addEventListener('click', function(){
    var v = String(email.value || '').trim().toLowerCase();
    if (!EMAIL_RE.test(v)){ msg(['Escriba su correo arriba y vuelva a tocar «Olvidé mi contraseña».'], 'bad'); try { email.focus(); } catch (x) {} return; }
    msg(['Enviando…']);
    sb.rpc('can_login', { p_email: v }).then(function(r){
      if (r.error) throw r.error;
      if (!r.data){ msg(['Este correo no tiene acceso.', 'Si trabaja con Blu, pídanos que lo agreguemos.'], 'bad'); return; }
      return sb.auth.resetPasswordForEmail(v, { redirectTo: location.origin + location.pathname }).then(function(x){
        if (x.error){ msg([friendlyAuthError(x.error)], 'bad'); return; }
        msg(['Le enviamos un correo a ' + v + ' con un enlace para elegir una contraseña nueva.', 'Sirve una sola vez y vence en 1 hora.']);
      });
    }).catch(function(){ msg(['No hay conexión con el servidor. Inténtelo de nuevo.'], 'bad'); });
  });
}

function wireMfa(){
  var box = $('#mfaBox'), inp = $('#authMfa');
  box.hidden = false; msg(['Esta cuenta tiene verificación en dos pasos.', 'Abra su app de autenticación y escriba el código de 6 dígitos.']);
  setTimeout(function(){ try { inp.focus({ preventScroll: true }); } catch (e) {} }, 400);
  box.addEventListener('submit', function(e){
    e.preventDefault();
    var code = String(inp.value || '').replace(/\D/g, '');
    if (code.length !== 6){ msg(['Escriba los 6 dígitos del código.'], 'bad'); return; }
    var btn = box.querySelector('button[type=submit]'); btn.disabled = true; msg(['Verificando…']);
    sb.auth.mfa.listFactors().then(function(f){
      if (f.error) throw f.error;
      var totp = (f.data && f.data.totp || []).filter(function(x){ return x.status === 'verified'; })[0];
      if (!totp) throw new Error('no factor');
      return sb.auth.mfa.challengeAndVerify({ factorId: totp.id, code: code });
    }).then(function(v){
      if (v.error){ btn.disabled = false; inp.value = ''; msg(['Ese código no es válido. Pruebe con el siguiente que muestre la app.'], 'bad'); return; }
      location.replace(location.pathname);
    }).catch(function(){ btn.disabled = false; msg(['No se pudo verificar. Inténtelo de nuevo.'], 'bad'); });
  });
  $('#mfaBack').addEventListener('click', signOut);
}

function wireSetPassword(){
  var box = $('#passBox'), p1 = $('#newPass'), p2 = $('#newPass2'), rules = $('#pwRules'), intro = $('#pwIntro');
  intro.textContent = urlType === 'recovery' ? 'Elija su contraseña nueva para ' + S.session.user.email + '.' : 'Bienvenido. Cree la contraseña con la que entrará a la Consola como ' + S.session.user.email + '.';
  box.hidden = false;
  function paint(){ var c = pwChecks(p1.value || '', p2.value || ''); Object.keys(c).forEach(function(k){ var li = rules.querySelector('[data-r=' + k + ']'); if (li) li.classList.toggle('ok', !!c[k]); }); }
  p1.addEventListener('input', paint); p2.addEventListener('input', paint);
  setTimeout(function(){ try { p1.focus({ preventScroll: true }); } catch (e) {} }, 400);
  box.addEventListener('submit', function(e){
    e.preventDefault();
    if (!pwOk(p1.value || '', p2.value || '')){ msg(['La contraseña todavía no cumple todos los puntos.'], 'bad'); return; }
    var btn = box.querySelector('button[type=submit]'); btn.disabled = true; msg(['Guardando…']);
    sb.auth.updateUser({ password: p1.value }).then(function(r){
      if (r.error){ btn.disabled = false; msg([friendlyAuthError(r.error)], 'bad'); return; }
      msg(['Listo. Entrando…']);
      setTimeout(function(){ location.replace(location.pathname); }, 600);
    });
  });
}

function afterStart(){
  var doors = $('#doors'), foot = $('.login-foot');
  if (!S.session){
    if (doors) doors.hidden = true;
    if (S.error) msg(['No hay conexión con el servidor. Recargue la página.'], 'bad');
    else if (hashErr) msg(['El enlace ya venció o ya se usó. Pida uno nuevo desde «Olvidé mi contraseña» o a su socio de Blu.'], 'bad');
    wireAuthForms();
    return;
  }
  if (S.mfaNeeded){ if (doors) doors.hidden = true; wireMfa(); return; }
  if (S.setPassword){ if (doors) doors.hidden = true; wireSetPassword(); return; }
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
    var sec = document.createElement('button'); sec.className = 'nav-btn'; sec.type = 'button'; sec.id = 'securityBtn';
    sec.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3l7 3v5c0 4.5-3 8.3-7 10-4-1.7-7-5.5-7-10V6z"/><path d="M9.5 12l1.8 1.8L15 10"/></svg><span>Seguridad</span>';
    sec.addEventListener('click', openSecurity);
    fb.parentNode.insertBefore(sec, b);
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
  function invite(email, after){
    U.toast('Enviando invitación…');
    sendInvite(email).then(function(kind){ U.toast(kind === 'reset' ? 'Enviado: enlace para crear una contraseña nueva' : 'Invitación enviada a ' + email); if (after) after(); })
      .catch(function(e){ U.toast('Acceso guardado, pero el correo no salió: ' + friendlyAuthError(e)); if (after) after(); });
  }
  function resendBtn(email){ return S.socio ? h('button', { class: 'btn small ghost', type: 'button', text: 'Reenviar invitación', onclick: function(){ invite(email); } }) : null; }
  var statusCache = {};
  function statusOf(email, el){
    if (statusCache[email] !== undefined){ el.textContent = statusCache[email]; return; }
    sb.rpc('access_status', { p_email: email }).then(function(r){ var t = (!r.error && r.data) ? r.data : ''; statusCache[email] = t; el.textContent = t; }, function(){});
  }
  var EMAIL = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
  function draw(){
    Promise.all([sb.from('team_members').select('email,nombre,rol').order('email'), sb.from('client_users').select('email,client_id,nombre').order('email')]).then(function(r){
      var team = r[0].data || [], cu = r[1].data || [], clients = U.clients(), names = {};
      clients.forEach(function(c){ names[c.id] = c.nombre; });
      body.innerHTML = '';
      body.appendChild(h('p', { class: 'muted', style: 'margin:0;line-height:1.55', text: 'Quién puede entrar y qué ve. Al agregar a alguien le llega una invitación por correo con un enlace de un solo uso para crear su contraseña. Después entra con su correo y su contraseña.' }));

      var tl = h('div', { class: 'list' });
      team.forEach(function(t){
        var self = S.session && t.email === S.session.user.email.toLowerCase();
        var rl = (t.rol === 'empleado') ? 'Empleado' : 'Socio';
        var st = h('span', { class: 's', text: '' }); if (!self) statusOf(t.email, st);
        var sub = h('span', { style: 'display:inline-flex;gap:8px;flex-wrap:wrap' }, h('span', { class: 's', text: rl }), st);
        var row = rowOf((t.nombre ? t.nombre + ' · ' : '') + t.email + (self ? ' (usted)' : ''), null, (self || !S.socio) ? null : function(){
          sb.from('team_members').delete().eq('email', t.email).then(function(x){ if (x.error) fail(x.error); else { U.toast('Acceso quitado'); draw(); } });
        });
        row.firstChild.appendChild(sub);
        if (S.socio && !self){
          var rs = h('select', { style: 'width:auto' }, h('option', { value: 'socio', text: 'Socio' }), h('option', { value: 'empleado', text: 'Empleado' }));
          rs.value = t.rol === 'empleado' ? 'empleado' : 'socio';
          rs.addEventListener('change', function(){ sb.from('team_members').update({ rol: rs.value }).eq('email', t.email).then(function(x){ if (x.error) fail(x.error); else { U.toast('Rol actualizado'); draw(); } }); });
          var acts = h('span', { style: 'display:inline-flex;gap:6px;align-items:center;flex-wrap:wrap;justify-content:flex-end' }, rs, resendBtn(t.email), row.lastChild);
          row.appendChild(acts);
        }
        tl.appendChild(row);
      });
      var te = h('input', { type: 'email', placeholder: 'nombre@agenciablu.com' }), tn = h('input', { type: 'text', placeholder: 'Nombre' });
      var tr = h('select', {}, h('option', { value: 'empleado', text: 'Empleado' }), h('option', { value: 'socio', text: 'Socio' }));
      var tf = h('form', { style: 'display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1fr) auto auto;gap:8px;align-items:end' },
        field('Correo', te), field('Nombre', tn), field('Rol', tr), h('button', { class: 'btn small primary', type: 'submit', text: 'Agregar' }));
      tf.addEventListener('submit', function(e){ e.preventDefault(); var v = te.value.trim().toLowerCase(); if (!EMAIL.test(v)) return;
        sb.from('team_members').insert({ email: v, nombre: tn.value.trim() || null, rol: tr.value }).then(function(x){ if (x.error) fail(x.error); else { te.value = ''; tn.value = ''; invite(v, draw); } }); });
      body.appendChild(h('section', { style: 'display:grid;gap:10px' }, h('div', { class: 'h2', text: 'Equipo' }), h('div', { class: 'muted', style: 'font-size:13px', text: S.socio ? 'Socios: ven todo y manejan los accesos. Empleados: trabajan en clientes, flujos y tareas, pero no agregan ni quitan accesos.' : 'Solo un socio puede agregar o quitar accesos.' }), tl, S.socio ? tf : null));

      var cl = h('div', { class: 'list' });
      if (!cu.length) cl.appendChild(h('div', { class: 'row', style: 'grid-template-columns:1fr' }, h('span', { class: 's', text: 'Todavía no hay accesos de clientes.' })));
      cu.forEach(function(c){
        var crow = rowOf((c.nombre ? c.nombre + ' · ' : '') + c.email, names[c.client_id] || c.client_id, !S.socio ? null : function(){
          sb.from('client_users').delete().eq('email', c.email).then(function(x){ if (x.error) fail(x.error); else { U.toast('Acceso quitado'); draw(); } });
        });
        var cst = h('span', { class: 's', text: '' }); statusOf(c.email, cst); crow.firstChild.appendChild(cst);
        if (S.socio){ var cacts = h('span', { style: 'display:inline-flex;gap:6px;align-items:center;flex-wrap:wrap;justify-content:flex-end' }, resendBtn(c.email), crow.lastChild); crow.appendChild(cacts); }
        cl.appendChild(crow);
      });
      var ce = h('input', { type: 'email', placeholder: 'gerente@cliente.com' }), cs = h('select', {});
      clients.sort(function(a, b){ return (a.nombre || '').localeCompare(b.nombre || ''); }).forEach(function(c){ cs.appendChild(h('option', { value: c.id, text: c.nombre })); });
      var cf = h('form', { style: 'display:grid;grid-template-columns:minmax(0,1.4fr) minmax(0,1fr) auto;gap:8px;align-items:end' },
        field('Correo', ce), field('Cliente', cs), h('button', { class: 'btn small primary', type: 'submit', text: 'Agregar' }));
      cf.addEventListener('submit', function(e){ e.preventDefault(); var v = ce.value.trim().toLowerCase(); if (!EMAIL.test(v) || !cs.value) return;
        sb.from('client_users').insert({ email: v, client_id: cs.value }).then(function(x){ if (x.error) fail(x.error); else { ce.value = ''; invite(v, draw); } }); });
      body.appendChild(h('section', { style: 'display:grid;gap:10px' }, h('div', { class: 'h2', text: 'Portales de clientes' }), h('div', { class: 'muted', style: 'font-size:13px', text: 'Ve solo el portal de su empresa, sin notas internas.' }), cl, S.socio ? cf : null,
        h('div', { class: 'muted', style: 'font-size:12.5px', text: 'Para entrar: consola.agenciablu.com con ese correo y la contraseña que cree desde la invitación.' })));
    });
  }
  draw(); U.openDrawer('Accesos', body);
}

/* ---------- Seguridad: own password and two-step verification ---------- */
function openSecurity(){
  var U = window.__ui; if (!U) return;
  var h = U.h, body = h('div', { style: 'display:grid;gap:24px;min-width:0;max-width:100%' });
  function field(label, input){ return h('div', { class: 'field', style: 'min-width:0' }, h('label', { text: label }), input); }
  // password
  var p1 = h('input', { type: 'password', autocomplete: 'new-password', placeholder: 'Mínimo 10 caracteres', style: 'width:100%;min-width:0;box-sizing:border-box' }), p2 = h('input', { type: 'password', autocomplete: 'new-password', style: 'width:100%;min-width:0;box-sizing:border-box' });
  var pf = h('form', { style: 'display:grid;gap:8px' }, field('Contraseña nueva', p1), field('Repítala', p2),
    h('div', { class: 'muted', style: 'font-size:12.5px', text: '10 caracteres o más, con mayúscula, minúscula y número.' }),
    h('button', { class: 'btn small primary', type: 'submit', text: 'Cambiar contraseña', style: 'justify-self:start' }));
  pf.addEventListener('submit', function(e){
    e.preventDefault();
    if (!pwOk(p1.value || '', p2.value || '')){ U.toast('La contraseña no cumple los requisitos o no coincide'); return; }
    sb.auth.updateUser({ password: p1.value }).then(function(r){ if (r.error) U.toast(friendlyAuthError(r.error)); else { p1.value = ''; p2.value = ''; U.toast('Contraseña cambiada'); } });
  });
  body.appendChild(h('section', { style: 'display:grid;gap:10px' }, h('div', { class: 'h2', text: 'Contraseña' }), pf));
  // two-step
  var mfaSec = h('section', { style: 'display:grid;gap:10px' }, h('div', { class: 'h2', text: 'Verificación en dos pasos' }));
  var mfaBody = h('div', { style: 'display:grid;gap:10px' }); mfaSec.appendChild(mfaBody); body.appendChild(mfaSec);
  function drawMfa(){
    mfaBody.innerHTML = '';
    sb.auth.mfa.listFactors().then(function(f){
      if (f.error) throw f.error;
      var on = (f.data && f.data.totp || []).filter(function(x){ return x.status === 'verified'; });
      var pending = (f.data && f.data.all || []).filter(function(x){ return x.status !== 'verified'; });
      if (on.length){
        mfaBody.appendChild(h('div', { class: 'row', style: 'grid-template-columns:minmax(0,1fr) auto' },
          h('span', { style: 'display:grid;gap:2px' }, h('span', { class: 't', text: 'Activada' }), h('span', { class: 's', text: 'Al entrar, además de la contraseña se pide el código de su app de autenticación.' })),
          h('button', { class: 'btn small ghost danger', type: 'button', text: 'Desactivar', onclick: function(){
            sb.auth.mfa.unenroll({ factorId: on[0].id }).then(function(r){ if (r.error) U.toast(friendlyAuthError(r.error)); else { U.toast('Verificación en dos pasos desactivada'); drawMfa(); } });
          } })));
        return;
      }
      mfaBody.appendChild(h('div', { class: 'muted', style: 'font-size:13px;line-height:1.5', text: 'Añade una segunda llave: un código de 6 dígitos que cambia cada 30 segundos en su teléfono (Google Authenticator, Microsoft Authenticator, 1Password). Aunque alguien consiga su contraseña, no podrá entrar.' }));
      mfaBody.appendChild(h('button', { class: 'btn small primary', type: 'button', text: 'Activar', style: 'justify-self:start', onclick: function(){
        Promise.all(pending.map(function(x){ return sb.auth.mfa.unenroll({ factorId: x.id }); })).then(function(){
          return sb.auth.mfa.enroll({ factorType: 'totp', friendlyName: 'Consola Blu' });
        }).then(function(r){
          if (r.error) throw r.error;
          var fid = r.data.id, qr = r.data.totp && r.data.totp.qr_code, secret = r.data.totp && r.data.totp.secret;
          mfaBody.innerHTML = '';
          mfaBody.appendChild(h('div', { class: 'muted', style: 'font-size:13px;line-height:1.5', text: '1. Abra su app de autenticación y escanee este código.' }));
          if (qr) mfaBody.appendChild(h('img', { src: qr, alt: 'Código QR', style: 'width:180px;height:180px;background:#fff;padding:8px;border-radius:8px;justify-self:start' }));
          mfaBody.appendChild(h('div', { class: 'muted', style: 'font-size:12px', text: 'Si no puede escanear, escriba esta clave en la app: ' + (secret || '') }));
          var code = h('input', { type: 'text', inputmode: 'numeric', autocomplete: 'one-time-code', maxlength: '6', placeholder: '000000', style: 'max-width:160px' });
          var vf = h('form', { style: 'display:grid;gap:8px' }, h('div', { class: 'muted', style: 'font-size:13px', text: '2. Escriba el código de 6 dígitos que muestra la app.' }), code, h('button', { class: 'btn small primary', type: 'submit', text: 'Confirmar', style: 'justify-self:start' }));
          vf.addEventListener('submit', function(e){
            e.preventDefault(); var c = String(code.value || '').replace(/\D/g, ''); if (c.length !== 6) return;
            sb.auth.mfa.challengeAndVerify({ factorId: fid, code: c }).then(function(v){ if (v.error){ U.toast('Código no válido. Pruebe con el siguiente.'); code.value = ''; } else { U.toast('Verificación en dos pasos activada'); drawMfa(); } });
          });
          mfaBody.appendChild(vf);
          setTimeout(function(){ try { code.focus(); } catch (e) {} }, 50);
        }).catch(function(e){ U.toast(friendlyAuthError(e)); });
      } }));
    }).catch(function(){ mfaBody.appendChild(h('div', { class: 'muted', text: 'No se pudo consultar el estado.' })); });
  }
  drawMfa();
  U.openDrawer('Seguridad', body);
}

/* The console itself lives in private storage: only signed-in team members and clients can download it. */
var APP_FILE = 'app-59a70bf234.js';
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
  var needsApp = !S.mfaNeeded && !S.setPassword && (S.role === 'team' || S.role === 'client');
  return (needsApp ? loadApp() : Promise.resolve()).then(function(){
    if (needsApp) window.__startApp();
    afterStart();
  }, function(e){
    console.warn(e); S.error = e; S.role = 'none';
    afterStart();
  });
});
})();
