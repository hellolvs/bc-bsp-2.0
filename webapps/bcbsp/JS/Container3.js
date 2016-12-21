WBBM.Container3 = WBBM.Class.create();
WBBM.Container3.prototype = {
	initialize: function(cnt_n, cnt_at) {
		var $T = this;
		$T.cnt_3 = WBBM.$(cnt_n);
		if (!$T.cnt_3) return;
		$T.cnt = $T.cnt_3.children[0];
		$T.Titles = $T.cnt.children[0].children;
		$T.Contents = $T.cnt.children[1].children;
		if ($T.cnt.children[0].nodeName == 'UL') {
			WBBM.each($T.Titles,
			function(j, i) {
				if (!$T.Contents[i]) {
					alert('lack of Contents,id:' + cnt_n);
					return;
				}
				WBBM.addEvent(j, 'mouseover', WBBM.bind($T, $T.cnt_chg, [j, $T.Contents[i]]));
			});
			WBBM.each($T.Contents,
			function(j, i) {
				if (!$T.Titles[i]) {
					alert('lack of Titles,id:' + cnt_n);
				}
				j.style.display = 'none';
			});
			if (cnt_at > 0) $T.go(cnt_at);
		}
	},
	pre_t: null,
	pre_c: null,
	go: function(cnt_at) {
		this.cnt_chg(this.Titles[cnt_at - 1], this.Contents[cnt_at - 1]);
	},
	cnt_chg: function(cnt_t, cnt_c) {
		if (!cnt_t && !cnt_c) return;
		if (this.pre_t) {
			if (this.pre_t == cnt_t) return;
			WBBM.delCls(this.pre_t, 'c_on');
			this.pre_c.style.display = 'none';
		}
		this.pre_t = cnt_t;
		this.pre_c = cnt_c;
		WBBM.addCls(cnt_t, 'c_on');
		cnt_c.style.display = 'block';
	}
}
WBBM.ready(function() {
	new WBBM.Container3('Container3', 1);
});