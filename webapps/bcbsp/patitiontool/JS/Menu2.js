WBBM.Menu2 = WBBM.Class.create();
WBBM.Menu2.prototype = {
	initialize: function(m_n, d) {
		var $T = this;
		$T.theMenu = WBBM.$(m_n);
		$T.lis = WBBM.$$(this.theMenu, 'ul', 0).children;
		$T.d = d;
		WBBM.each($T.lis,
		function(l, i) {
			var uls = WBBM.$$(l, 'ul');
			if (!uls.length) {
				WBBM.addEvent(l, 'mouseenter',
				function() {
					WBBM.addCls(this, 'm_hover');
				});
				WBBM.addEvent(l, 'mouseleave',
				function() {
					WBBM.delCls(this, 'm_hover');
				});
			} else {
				WBBM.addCls(WBBM.$$(l, 'a', 0), 'm_arw');
				WBBM.each(uls,
				function(k, j) {
					var kp = k.parentNode;
					k.cs = 0;
					WBBM.each(k.children,
					function(n) {
						if (n.className != 'm_split') k.cs += 41;
						else k.cs += 2;
					});
					var sdw_t = document.createElement('div');
					sdw_t.className = 'Menu2_css_sdw Menu2_m_subsdw';
					kp.appendChild(sdw_t);
					WBBM.setOpacity(k, 0);
					WBBM.addEvent(kp, 'mouseenter', WBBM.bind($T, $T.m_mover, [kp, k, sdw_t, j]));
					WBBM.addEvent(kp, 'mouseleave', WBBM.bind($T, $T.m_mout, [kp, k, sdw_t, j]));
					WBBM.each(WBBM.$$(k, 'li'),
					function(e) {
						if (!WBBM.$$(e, 'ul').length) WBBM.addEvent(e, 'click', WBBM.bind($T, $T.m_mout, [kp, k, sdw_t, j, 1]));
						else WBBM.addCls(WBBM.$$(e, 'a', 0), 'm_subarw');
					});
				});
			}
		});
	},
	tw: function(t, b, c, d) {
		if ((t /= d) < (1 / 2.75)) {
			return c * (7.5625 * t * t) + b
		} else if (t < (2 / 2.75)) {
			return c * (7.5625 * (t -= (1.5 / 2.75)) * t + .75) + b
		} else if (t < (2.5 / 2.75)) {
			return c * (7.5625 * (t -= (2.25 / 2.75)) * t + .9375) + b
		} else {
			return c * (7.5625 * (t -= (2.625 / 2.75)) * t + .984375) + b
		}
	},
	showSdw: function(ec, s) {
		var ss = s.style;
		ss.top = ec.offsetTop + 'px';
		ss.left = ec.offsetLeft + 'px';
		ss.width = ec.offsetWidth + 'px';
		ss.height = ec.offsetHeight + 'px';
		ss.display = 'block';
	},
	m_mover: function(ep, ec, s, wh) {
		var $T = this;
		WBBM.animate(ec, {
			keys: ['opacity', 'height', 'width'],
			values: [1, ec.cs, 118],
			tween: $T.tw,
			onstart: function() {
				if (!WBBM.hasCls(ep, 'm_active')) {
					WBBM.delCls(WBBM.$$(ep, 'a', 0), 'm_default');
					WBBM.delCls(ep, 'm_out');
				}
				WBBM.addCls(ep, 'm_hover');
				WBBM.addCls(WBBM.$$(ep, 'a', 0), 'm_hover');
				ec.style.visibility = ep.parentNode.style.overflow = 'visible';
			},
			onmove: function(m) {
				ep.parentNode.over = 1;
			},
			onfinish: function() {
				$T.showSdw(ec, s);
			}
		},
		$T.d);
	},
	m_mout: function(ep, ec, s, wh, x) {
		if (x) WBBM.stopBubble(ec);
		var $T = this;
		WBBM.animate(ec, {
			keys: ['opacity', 'height', 'width'],
			values: [0, 0, 0],
			tween: this.tw,
			onstart: function() {
				s.style.display = 'none';
				if (!WBBM.hasCls(ep, 'm_active')) {
					WBBM.addCls(WBBM.$$(ep, 'a', 0), 'm_default');
					WBBM.addCls(ep, 'm_out');
				}
				WBBM.delCls(WBBM.$$(ep, 'a', 0), 'm_hover');
				var eculs = WBBM.$$(ec, 'ul');
				WBBM.each(eculs,
				function(e, i) {
					e.style.visibility = 'hidden';
				});
			},
			onmove: function(m, t) {
				if (!t) ec.style.overflow = 'hidden';
			},
			onfinish: function() {
				WBBM.delCls(ep, 'm_hover');
				ec.style.visibility = 'hidden';
			}
		},
		$T.d);
	}
}
WBBM.ready(function() {
	new WBBM.Menu2('Menu2', 400);
});