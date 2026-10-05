(function () {
  'use strict';

  // ----------------------------- decks -----------------------------
  var DECKS = {
    fibonacci:  { name: 'Фибоначчи', cards: ['0','1','2','3','5','8','13','21','34','55','?','☕'] },
    modified:   { name: 'Mod. Fibonacci', cards: ['0','½','1','2','3','5','8','13','20','40','100','?'] },
    sequential: { name: 'Последовательные', cards: ['1','2','3','4','5','6','7','8','9','10'] },
    tshirt:     { name: 'T-shirt', cards: ['XS','S','M','L','XL','XXL','?'] },
    powers:     { name: 'Степени 2', cards: ['1','2','4','8','16','32','64'] }
  };

  // Публичные WebSocket-брокеры (без аккаунтов). Пробуем по очереди.
  var BROKERS = [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081/mqtt'
  ];
  var NS = 'pp2';

  // ----------------------------- utils -----------------------------
  function $(s) { return document.querySelector(s); }
  function esc(s) {
    return String(s == null ? '' : s)
      .split('&').join('&amp;').split('<').join('&lt;')
      .split('>').join('&gt;').split('"').join('&quot;');
  }
  function uid(n) {
    var a = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var b = crypto.getRandomValues(new Uint8Array(n));
    var s = '';
    for (var i = 0; i < n; i++) { s += a[b[i] % a.length]; }
    return s;
  }
  function b64url(bytes) {
    var s = '';
    for (var i = 0; i < bytes.length; i++) { s += String.fromCharCode(bytes[i]); }
    return btoa(s).split('+').join('-').split('/').join('_').split('=').join('');
  }
  function unb64url(str) {
    var s = String(str).split('-').join('+').split('_').join('/');
    while (s.length % 4) { s += '='; }
    var bin = atob(s);
    var out = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) { out[i] = bin.charCodeAt(i); }
    return out;
  }

  var KEYCACHE = {};
  async function channelKey(secret) {
    if (KEYCACHE[secret]) { return KEYCACHE[secret]; }
    var raw = await crypto.subtle.digest('SHA-256', new TextEncoder().encode('planning-poker:' + secret));
    var key = await crypto.subtle.importKey('raw', raw, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
    KEYCACHE[secret] = key;
    return key;
  }
  async function seal(secret, obj) {
    var key = await channelKey(secret);
    var iv = crypto.getRandomValues(new Uint8Array(12));
    var ct = await crypto.subtle.encrypt({ name: 'AES-GCM', iv: iv }, key, new TextEncoder().encode(JSON.stringify(obj)));
    var out = new Uint8Array(12 + ct.byteLength);
    out.set(iv, 0);
    out.set(new Uint8Array(ct), 12);
    return b64url(out);
  }
  async function openSealed(secret, str) {
    var key = await channelKey(secret);
    var buf = unb64url(str);
    var iv = buf.slice(0, 12);
    var ct = buf.slice(12);
    var pt = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: iv }, key, ct);
    return JSON.parse(new TextDecoder().decode(pt));
  }

  function toast(msg) {
    var t = $('#toast');
    if (!t) { return; }
    t.className = 'toast';
    t.textContent = msg;
    clearTimeout(t._t);
    t._t = setTimeout(function () { t.className = ''; }, 1900);
  }
  function copyText(text) {
    function fallback() {
      var ta = document.createElement('textarea');
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); toast('Скопировано'); } catch (e) { toast('Не удалось скопировать'); }
      document.body.removeChild(ta);
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () { toast('Ссылка скопирована'); }, fallback);
    } else { fallback(); }
  }

  // ----------------------------- state -----------------------------
  var App = {
    view: 'landing',
    room: null, secret: null, hostToken: null, isHost: false,
    myId: null, myName: '',
    deckKey: 'fibonacci', custom: '',
    client: null, broker: '', connected: false,
    state: null, myVote: null, votes: {}, taskDraft: '',
    joinTimer: null, lastRound: -1
  };

  function readHash() {
    var h = location.hash || '';
    if (h.charAt(0) === '#') { h = h.slice(1); }
    var o = {};
    if (!h) { return o; }
    h.split('&').forEach(function (p) {
      var i = p.indexOf('=');
      if (i < 0) { return; }
      try {
        o[decodeURIComponent(p.slice(0, i))] = decodeURIComponent(p.slice(i + 1));
      } catch (e) { /* ignore */ }
    });
    return o;
  }
  function tState() { return NS + '/' + App.room + '/s'; }
  function tIntent() { return NS + '/' + App.room + '/i'; }

  function participantLink() {
    return location.origin + location.pathname + '#r=' + encodeURIComponent(App.room) + '&k=' + encodeURIComponent(App.secret);
  }
  function parseCustom(s) {
    return String(s || '').split(',').map(function (x) { return x.trim(); })
      .filter(function (x) { return x !== ''; }).slice(0, 30);
  }

  // ----------------------------- transport -----------------------------
  function connect(list, idx) {
    if (idx >= list.length) {
      App.connected = false; render();
      toast('Не удалось подключиться к серверу синхронизации');
      return;
    }
    var url = list[idx];
    App.broker = url;
    var c;
    try {
      c = mqtt.connect(url, { clientId: 'pp_' + uid(12), clean: true, connectTimeout: 6000, reconnectPeriod: 0, keepalive: 30 });
    } catch (e) { connect(list, idx + 1); return; }
    var settled = false;
    var timer = setTimeout(function () {
      if (settled) { return; }
      settled = true;
      try { c.end(true); } catch (e) { /* ignore */ }
      connect(list, idx + 1);
    }, 8000);
    c.on('connect', function () {
      if (settled) { return; }
      settled = true;
      clearTimeout(timer);
      App.client = c;
      App.connected = true;
      onConnected();
      render();
    });
    c.on('error', function () {
      if (settled) { return; }
      settled = true;
      clearTimeout(timer);
      try { c.end(true); } catch (e) { /* ignore */ }
      connect(list, idx + 1);
    });
    c.on('close', function () {
      if (!settled) { return; }
      App.connected = false; render();
      setTimeout(reconnect, 1500);
    });
  }
  function reconnect() {
    if (App.client) { try { App.client.end(true); } catch (e) { /* ignore */ } App.client = null; }
    if (App.room) { connect(BROKERS, 0); }
  }

  function onConnected() {
    var c = App.client;
    c.removeAllListeners('message');
    c.on('message', onMessage);
    c.subscribe(tState(), { qos: 1 });
    if (App.isHost) {
      c.subscribe(tIntent(), { qos: 1 });
      if (App.state) { pubState(); } else { ensureHostState(); }
    } else {
      startJoinLoop();
    }
  }

  async function pubState() {
    if (!App.isHost || !App.state || !App.connected || !App.client) { return; }
    App.state.updated = Date.now();
    try {
      var p = await seal(App.secret, App.state);
      App.client.publish(tState(), p, { retain: true, qos: 1 });
    } catch (e) { /* ignore */ }
  }
  async function sendIntent(m) {
    if (!App.connected || !App.client) { return; }
    try {
      var p = await seal(App.secret, m);
      App.client.publish(tIntent(), p, { qos: 1 });
    } catch (e) { /* ignore */ }
  }

  function onMessage(topic, payload) {
    var str = payload.toString();
    if (topic === tState()) {
      openSealed(App.secret, str).then(function (s) {
        if (!s || !s.v) { return; }
        var prev = App.state;
        App.state = s;
        if (prev && prev.round && s.round && prev.round.n !== s.round.n) { App.myVote = null; }
        if (App.isHost) { ensureSelfPresent(s); }
        render();
      }).catch(function () { /* not ours / bad key */ });
    } else if (topic === tIntent() && App.isHost) {
      openSealed(App.secret, str).then(handleIntent).catch(function () { /* ignore */ });
    }
  }

  function ensureSelfPresent(s) {
    if (!s.hostId) { s.hostId = App.myId; }
    if (s.hostId !== App.myId) { return; }
    var has = s.participants.some(function (p) { return p.id === App.myId; });
    if (!has) {
      s.participants.push({ id: App.myId, name: App.myName || 'Админ', host: true });
      pubState();
    }
  }

  function ensureHostState() {
    setTimeout(function () {
      if (App.state) { return; }
      var deck = App.deckKey === 'custom'
        ? { name: 'Свои карточки', cards: parseCustom(App.custom) }
        : DECKS[App.deckKey];
      App.state = {
        v: 1, hostId: App.myId, hostName: App.myName || 'Админ',
        deck: deck,
        participants: [{ id: App.myId, name: App.myName || 'Админ', host: true }],
        task: { id: null, title: '' },
        round: { n: 0, mode: 'idle', voted: [], revealed: [] },
        history: [], updated: Date.now()
      };
      pubState();
      render();
    }, 1500);
  }

  function inRoom() {
    return !!(App.state && App.state.participants && App.state.participants.some(function (p) { return p.id === App.myId; }));
  }
  function startJoinLoop() {
    if (App.joinTimer) { return; }
    var tries = 0;
    var kick = function () {
      if (inRoom() || tries++ > 24) { clearInterval(App.joinTimer); App.joinTimer = null; return; }
      sendIntent({ type: 'join', id: App.myId, name: App.myName });
    };
    kick();
    App.joinTimer = setInterval(kick, 2500);
  }

  // ----------------------------- host logic -----------------------------
  function handleIntent(m) {
    var s = App.state;
    if (!s || !m || !m.type) { return; }
    if (m.type === 'join') {
      if (!s.participants.some(function (p) { return p.id === m.id; })) {
        s.participants.push({ id: m.id, name: String(m.name || 'Участник').slice(0, 40), host: false });
        pubState(); render();
      }
    } else if (m.type === 'leave') {
      s.participants = s.participants.filter(function (p) { return p.id !== m.id; });
      s.round.voted = s.round.voted.filter(function (x) { return x !== m.id; });
      delete App.votes[m.id];
      pubState(); render();
    } else if (m.type === 'vote') {
      if (s.round.mode !== 'voting' || m.round !== s.round.n) { return; }
      if (!s.participants.some(function (p) { return p.id === m.id; })) { return; }
      if (!s.deck.cards.some(function (c) { return c === m.card; })) { return; }
      App.votes[m.id] = m.card;
      if (s.round.voted.indexOf(m.id) < 0) { s.round.voted.push(m.id); }
      pubState(); render();
    }
  }

  function createSession() {
    if (!App.myName.trim()) { toast('Введите имя'); $('#nameInput').focus(); return; }
    var cards = App.deckKey === 'custom' ? parseCustom(App.custom) : DECKS[App.deckKey].cards;
    if (!cards || cards.length < 2) { toast('Нужно минимум 2 карточки'); return; }
    App.room = 'r' + uid(10);
    App.secret = uid(32);
    App.hostToken = uid(24);
    App.isHost = true;
    App.view = 'room';
    var deck = App.deckKey === 'custom' ? { name: 'Свои карточки', cards: cards } : DECKS[App.deckKey];
    App.state = {
      v: 1, hostId: App.myId, hostName: App.myName,
      deck: deck,
      participants: [{ id: App.myId, name: App.myName, host: true }],
      task: { id: null, title: '' },
      round: { n: 0, mode: 'idle', voted: [], revealed: [] },
      history: [], updated: Date.now()
    };
    try { localStorage.setItem('pp_name', App.myName); } catch (e) { /* ignore */ }
    location.hash = '#r=' + encodeURIComponent(App.room) + '&k=' + encodeURIComponent(App.secret) + '&h=' + encodeURIComponent(App.hostToken);
    render();
    connect(BROKERS, 0);
  }

  function joinRoom() {
    if (!App.myName.trim()) { toast('Введите имя'); $('#nameInput').focus(); return; }
    try { localStorage.setItem('pp_name', App.myName); } catch (e) { /* ignore */ }
    App.view = 'room';
    render();
    connect(BROKERS, 0);
  }

  function doLeave() {
    if (!App.isHost && App.connected) { sendIntent({ type: 'leave', id: App.myId }); }
    setTimeout(function () { location.href = location.origin + location.pathname; }, 120);
  }

  function startVoting() {
    var s = App.state;
    var inp = $('#taskInput');
    var title = ((inp && inp.value) || App.taskDraft || '').trim();
    if (!title) { toast('Введите название задачи'); return; }
    App.taskDraft = '';
    s.task = { id: uid(8), title: title };
    s.round = { n: s.round.n + 1, mode: 'voting', voted: [], revealed: [] };
    App.votes = {}; App.myVote = null;
    pubState(); render();
  }
  function castVote(card) {
    var s = App.state;
    if (!s || s.round.mode !== 'voting') { return; }
    App.myVote = card;
    if (App.isHost) {
      App.votes[App.myId] = card;
      if (s.round.voted.indexOf(App.myId) < 0) { s.round.voted.push(App.myId); }
      pubState();
    } else {
      sendIntent({ type: 'vote', id: App.myId, name: App.myName, round: s.round.n, card: card });
    }
    render();
  }
  function reveal() {
    var s = App.state;
    s.round.revealed = s.participants.map(function (p) {
      return { id: p.id, name: p.name, card: (App.votes[p.id] != null ? App.votes[p.id] : null) };
    });
    s.round.mode = 'revealed';
    pubState(); render();
  }
  function restart() {
    var s = App.state;
    s.round = { n: s.round.n + 1, mode: 'voting', voted: [], revealed: [] };
    App.votes = {}; App.myVote = null;
    pubState(); render();
  }
  function setEstimate(card) {
    var s = App.state;
    s.history.push({ title: s.task.title || 'Задача', estimate: card, at: Date.now() });
    s.task = { id: null, title: '' };
    s.round = { n: s.round.n, mode: 'idle', voted: [], revealed: [] };
    App.votes = {}; App.myVote = null;
    pubState(); render();
  }
  function newTask() {
    var s = App.state;
    s.task = { id: null, title: '' };
    s.round = { n: s.round.n, mode: 'idle', voted: [], revealed: [] };
    App.votes = {}; App.myVote = null;
    pubState(); render();
  }
  function shareLink() {
    var url = participantLink();
    if (navigator.share) {
      navigator.share({ title: 'Planning Poker', text: 'Присоединяйтесь к оценке задач', url: url }).catch(function () { copyText(url); });
    } else { copyText(url); }
  }

  // ----------------------------- fragments -----------------------------
  function modeTag(m) {
    if (m === 'voting') { return '<span class="tag vote">идёт голосование</span>'; }
    if (m === 'revealed') { return '<span class="tag rev">карты вскрыты</span>'; }
    return '<span class="tag idle">ожидание</span>';
  }
  function cardButtons() {
    var s = App.state;
    return s.deck.cards.map(function (c) {
      var sel = (App.myVote != null && App.myVote === c);
      var dis = s.round.mode !== 'voting';
      return '<div class="pcard' + (sel ? ' sel' : '') + (dis ? ' dis' : '') + '" data-act="card" data-card="' + esc(c) + '">' + esc(c) + '</div>';
    }).join('');
  }
  function estimateButtons() {
    var s = App.state;
    return s.deck.cards.map(function (c) {
      return '<div class="pcard" data-act="estimate" data-card="' + esc(c) + '">' + esc(c) + '</div>';
    }).join('');
  }
  function analysis(revealed) {
    var cards = revealed.map(function (r) { return r.card; }).filter(function (c) { return c != null; });
    var nums = cards.map(Number).filter(function (n) { return !isNaN(n); });
    var allSame = cards.length > 0 && cards.every(function (c) { return c === cards[0]; });
    var avg = nums.length ? (nums.reduce(function (a, b) { return a + b; }, 0) / nums.length) : null;
    return {
      allSame: allSame, avg: avg,
      min: nums.length ? Math.min.apply(null, nums) : null,
      max: nums.length ? Math.max.apply(null, nums) : null
    };
  }
  function peopleHtml() {
    var s = App.state;
    var mode = s.round.mode;
    var rev = {};
    (s.round.revealed || []).forEach(function (r) { rev[r.id] = r.card; });
    return s.participants.map(function (p) {
      var badge = '';
      if (mode === 'voting') {
        badge = s.round.voted.indexOf(p.id) >= 0 ? '<span class="badge ok">✓ готов</span>' : '<span class="badge">думает…</span>';
      } else if (mode === 'revealed') {
        badge = '<span class="reveal">' + esc(rev[p.id] == null ? '–' : rev[p.id]) + '</span>';
      }
      return '<div class="person"><div class="av">' + esc((p.name || '?').charAt(0).toUpperCase()) + '</div><div class="nm">' + esc(p.name) + (p.host ? ' 👑' : '') + '</div>' + badge + '</div>';
    }).join('');
  }
  function revealCards() {
    var s = App.state;
    return (s.round.revealed || []).map(function (r) {
      return '<div class="person"><div class="av">' + esc((r.name || '?').charAt(0).toUpperCase()) + '</div><div class="nm">' + esc(r.name) + '</div><div class="reveal">' + esc(r.card == null ? '–' : r.card) + '</div></div>';
    }).join('');
  }
  function historyHtml() {
    var s = App.state;
    var h = (s.history || []).slice().reverse();
    if (!h.length) { return ''; }
    return h.map(function (x) {
      var d = new Date(x.at);
      var t = ('0' + d.getHours()).slice(-2) + ':' + ('0' + d.getMinutes()).slice(-2);
      return '<div class="hitem"><span>' + esc(x.title) + '</span><span><span class="e">' + esc(x.estimate) + '</span> <span class="muted small">' + t + '</span></span></div>';
    }).join('');
  }
  function deckTiles() {
    var keys = ['fibonacci', 'modified', 'sequential', 'tshirt', 'powers', 'custom'];
    return keys.map(function (k) {
      var d = k === 'custom' ? { name: 'Свои карточки', cards: ['…'] } : DECKS[k];
      var mini = d.cards.slice(0, 6).map(function (c) { return '<span>' + esc(c) + '</span>'; }).join('');
      return '<div class="deck' + (App.deckKey === k ? ' sel' : '') + '" data-act="deck" data-deck="' + k + '"><div class="nm">' + esc(d.name) + '</div><div class="mini">' + mini + '</div></div>';
    }).join('');
  }

  // ----------------------------- views -----------------------------
  function viewLanding() {
    return '<div class="wrap">' +
      '<div class="hero"><h1>🃏 Planning Poker</h1><p class="muted">Командная оценка задач в реальном времени. Создайте сессию — участники подключатся по ссылке, без регистрации.</p></div>' +
      '<div class="grid" style="grid-template-columns:repeat(auto-fit,minmax(300px,1fr));margin-top:8px">' +
        '<div class="card"><h3>1. Ваше имя</h3>' +
          '<input id="nameInput" type="text" placeholder="Например, Андрей" value="' + esc(App.myName) + '" autocomplete="off">' +
          '<h3 style="margin-top:18px">2. Колода</h3>' +
          '<div class="decks">' + deckTiles() + '</div>' +
          '<div id="customWrap" style="margin-top:12px;display:' + (App.deckKey === 'custom' ? 'block' : 'none') + '"><label>Свои карточки (через запятую)</label><input id="customInput" type="text" placeholder="0, 1, 2, 3, 5, 8, ?" value="' + esc(App.custom) + '"></div>' +
          '<div style="margin-top:20px"><button class="primary big" data-act="create" style="width:100%">Создать сессию</button></div>' +
        '</div>' +
        '<div class="card"><h3>Как это работает</h3>' +
          '<ol class="muted" style="padding-left:18px;line-height:1.9">' +
            '<li>Создаёте сессию и выбираете колоду карт.</li>' +
            '<li>Делитесь ссылкой с командой.</li>' +
            '<li>Админ задаёт задачу и запускает голосование.</li>' +
            '<li>Все выбирают карту — оценки синхронизируются мгновенно.</li>' +
            '<li>«Вскрыть карты», обсуждение и финальная оценка.</li>' +
          '</ol>' +
          '<p class="muted small">Данные передаются по WebSocket и шифруются ключом, зашитым в ссылку. Сервер видит только шифротекст.</p>' +
        '</div>' +
      '</div>' +
      '<footer>Никакой регистрации • Работает в браузере на телефоне и ПК</footer>' +
    '</div>';
  }

  function viewJoin() {
    return '<div class="wrap"><div class="center" style="min-height:62vh"><div class="card" style="max-width:430px;width:100%">' +
      '<h2 style="margin-top:0">Присоединиться к сессии</h2>' +
      '<p class="muted small">Вас пригласили в командную оценку. Укажите имя, чтобы продолжить.</p>' +
      '<label>Ваше имя</label>' +
      '<input id="nameInput" type="text" placeholder="Ваше имя" value="' + esc(App.myName) + '" autocomplete="off">' +
      '<div style="margin-top:16px"><button class="primary big" data-act="join" style="width:100%">Войти</button></div>' +
    '</div></div></div>';
  }

  function viewRoom() {
    var s = App.state;
    var head = '<div class="spread"><div class="row">' +
      '<h1 style="font-size:20px;margin:0">🃏 Planning Poker</h1>' +
      '<span class="chip"><span class="dot ' + (App.connected ? 'on' : 'off') + '"></span>' + (App.connected ? 'online' : 'подключение…') + '</span>' +
      (App.isHost ? '<span class="tag vote">вы админ</span>' : '') +
      '</div><div class="row">' +
      (App.isHost ? '<button class="ghost" data-act="copy">🔗 Ссылка участникам</button>' : '') +
      '<button class="ghost" data-act="leave">Выйти</button>' +
      '</div></div>';

    if (!s || !s.participants) {
      return '<div class="wrap">' + head + '<div class="center" style="min-height:44vh"><div style="text-align:center"><div class="spin"></div><div class="muted">Подключение к сессии…</div></div></div></div>';
    }

    var mode = s.round.mode;
    var n = s.participants.length;
    var voted = s.round.voted.length;
    var main = '';

    if (mode === 'idle') {
      main = App.isHost
        ? '<label>Название задачи</label><div class="row"><input id="taskInput" type="text" placeholder="Что оцениваем?" value="' + esc(App.taskDraft) + '" style="flex:1;min-width:220px" autocomplete="off"><button class="primary" data-act="start">🚀 Запустить голосование</button></div>'
        : '<p class="muted">Ожидание, пока администратор создаст задачу…</p>';
    } else if (mode === 'voting') {
      main = '<div class="muted small" style="margin-bottom:10px">' +
        (App.isHost ? ('Проголосовали: ' + voted + ' из ' + n + '. ' + (voted >= n && n > 0 ? 'Можно вскрывать карты.' : 'Дождитесь остальных.')) : 'Выберите свою оценку') +
        '</div><div class="cards">' + cardButtons() + '</div>' +
        (App.isHost ? '<div style="margin-top:16px"><button class="primary big" data-act="reveal">🃏 Вскрыть карты</button></div>' : '');
    } else if (mode === 'revealed') {
      var a = analysis(s.round.revealed || []);
      main = '<div class="stat" style="margin:4px 0 14px">' +
        '<div class="box"><div class="muted small">Совпадение</div><div class="v">' + (a.allSame ? '✅ да' : '❌ нет') + '</div></div>' +
        '<div class="box"><div class="muted small">Среднее</div><div class="v">' + (a.avg == null ? '—' : (Math.round(a.avg * 10) / 10)) + '</div></div>' +
        '<div class="box"><div class="muted small">Мин / Макс</div><div class="v">' + (a.min == null ? '—' : a.min) + ' / ' + (a.max == null ? '—' : a.max) + '</div></div>' +
        '</div><div class="people">' + revealCards() + '</div>';
      if (App.isHost) {
        main += '<div style="margin-top:18px"><div class="muted small">Сохранить оценку и перейти к новой задаче:</div><div class="cards" style="margin-top:10px">' + estimateButtons() + '</div>' +
          '<div class="row" style="margin-top:14px"><button class="ghost" data-act="restart">🔁 Переголосовать</button><button class="ghost" data-act="newtask">⏭ Без оценки, новая задача</button></div></div>';
      } else {
        main += '<p class="muted small" style="margin-top:14px">Ожидание решения администратора…</p>';
      }
    }

    var hist = historyHtml();
    return '<div class="wrap">' + head +
      '<div class="grid" style="margin-top:16px;grid-template-columns:1fr">' +
        '<div class="card"><div class="spread"><div><div class="muted small">Текущая задача</div><h2 style="margin:2px 0 0">' + esc(s.task.title || '—') + '</h2></div>' + modeTag(mode) + '</div><div style="margin-top:16px">' + main + '</div></div>' +
        '<div class="card"><div class="spread"><h3 style="margin:0">Участники (' + n + ')</h3><span class="muted small">колода: ' + esc(s.deck.name) + '</span></div><div class="people" style="margin-top:12px">' + peopleHtml() + '</div></div>' +
        (hist ? '<div class="card"><h3 style="margin-top:0">История оценок</h3><div class="hist">' + hist + '</div></div>' : '') +
      '</div>' +
      '<footer>Real-time синхронизация через WebSocket • данные шифруются</footer>' +
    '</div>';
  }

  // ----------------------------- render -----------------------------
  function render() {
    var app = $('#app');
    if (!app) { return; }
    var act = document.activeElement;
    var aid = act && act.id ? act.id : null;
    var pos = null;
    try { if (act && act.selectionStart != null) { pos = act.selectionStart; } } catch (e) { /* ignore */ }

    if (App.view === 'landing') { app.innerHTML = viewLanding(); }
    else if (App.view === 'join') { app.innerHTML = viewJoin(); }
    else if (App.view === 'room') { app.innerHTML = viewRoom(); }
    else { app.innerHTML = '<div class="center"><div><div class="spin"></div><div class="muted">Загрузка…</div></div></div>'; }

    if (aid) {
      var node = document.getElementById(aid);
      if (node) {
        node.focus();
        try { if (pos != null && node.setSelectionRange) { node.setSelectionRange(pos, pos); } } catch (e) { /* ignore */ }
      }
    }
  }

  // ----------------------------- events -----------------------------
  function onClick(e) {
    var t = e.target;
    var n = (t && t.closest) ? t.closest('[data-act]') : null;
    if (!n) { return; }
    var a = n.getAttribute('data-act');
    if (a === 'deck') { App.deckKey = n.getAttribute('data-deck'); render(); }
    else if (a === 'create') { createSession(); }
    else if (a === 'join') { joinRoom(); }
    else if (a === 'card') { castVote(n.getAttribute('data-card')); }
    else if (a === 'start') { startVoting(); }
    else if (a === 'reveal') { reveal(); }
    else if (a === 'restart') { restart(); }
    else if (a === 'estimate') { setEstimate(n.getAttribute('data-card')); }
    else if (a === 'newtask') { newTask(); }
    else if (a === 'copy') { copyText(participantLink()); }
    else if (a === 'share') { shareLink(); }
    else if (a === 'leave') { doLeave(); }
  }
  function onInput(e) {
    var id = e.target && e.target.id;
    if (id === 'nameInput') {
      App.myName = e.target.value;
      try { localStorage.setItem('pp_name', App.myName); } catch (x) { /* ignore */ }
    } else if (id === 'customInput') {
      App.custom = e.target.value;
    } else if (id === 'taskInput') {
      App.taskDraft = e.target.value;
    }
  }
  function onKey(e) {
    if (e.key !== 'Enter') { return; }
    var id = e.target && e.target.id;
    if (id === 'nameInput') {
      if (App.view === 'landing') { createSession(); } else if (App.view === 'join') { joinRoom(); }
    } else if (id === 'taskInput' && App.isHost) { startVoting(); }
  }

  // ----------------------------- boot -----------------------------
  function boot() {
    try {
      App.myId = sessionStorage.getItem('pp_id') || ('u' + uid(10));
      sessionStorage.setItem('pp_id', App.myId);
    } catch (e) { App.myId = 'u' + uid(10); }
    try { App.myName = localStorage.getItem('pp_name') || ''; } catch (e) { App.myName = ''; }

    var f = readHash();
    if (f.r && f.k) {
      App.room = f.r; App.secret = f.k; App.hostToken = f.h || null; App.isHost = !!f.h;
      App.view = 'join';
    } else {
      App.view = 'landing';
    }

    document.addEventListener('click', onClick);
    document.addEventListener('input', onInput);
    document.addEventListener('keydown', onKey);
    window.addEventListener('beforeunload', function () {
      if (App.room && !App.isHost && App.myId && App.connected) { sendIntent({ type: 'leave', id: App.myId }); }
    });

    render();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
