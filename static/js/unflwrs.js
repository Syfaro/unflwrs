const rtf = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });

const units = {
  year: 24 * 60 * 60 * 1000 * 365,
  month: 24 * 60 * 60 * 1000 * 365 / 12,
  day: 24 * 60 * 60 * 1000,
  hour: 60 * 60 * 1000,
  minute: 60 * 1000,
  second: 1000,
};

function getRelativeTime(toDate, fromDate = new Date()) {
  const elapsed = toDate - fromDate;

  for (const unit in units) {
    if (Math.abs(elapsed) > units[unit] || unit === 'second') {
      return rtf.format(Math.round(elapsed / units[unit]), unit);
    }
  }
}

function updateRelativeTimes() {
  [...document.querySelectorAll('.relative-time[data-timestamp]')].forEach((elem) => {
    if (!elem.dataset.replacedText) {
      elem.title = elem.textContent.trim();
      elem.dataset.replacedText = true;
    }

    const timestamp = parseInt(elem.dataset.timestamp, 10);
    const date = new Date(timestamp * 1000);

    elem.textContent = getRelativeTime(date);
  });

  setTimeout(updateRelativeTimes, 1000 * 15);
}

updateRelativeTimes();

if (document.querySelector('h2[data-missing="yes"]')) {
  console.debug('Missing data, reloading in 60 seconds');

  setTimeout(() => {
    window.location.reload();
  }, 60 * 1000);
}

if (document.querySelector('main[data-page="feed"]')) {
  displayGraph().then(() => console.log('Updated graph'));
}

async function displayGraph() {
  const resp = await fetch('/feed/graph');
  const data = await resp.json();

  if (data.length < 2) {
    console.debug('Not enough data for graph');
    return;
  }

  const entries = data.map((row) => {
    return {
      x: new Date(row[0] * 1000),
      y: row[1],
    }
  });

  console.debug(entries);

  new Chartist.Line('.ct-chart', {
    series: [
      {
        name: 'followers',
        data: entries,
      }
    ]
  }, {
    axisY: {
      onlyInteger: true
    },
    axisX: {
      type: Chartist.FixedScaleAxis,
      divisor: 5,
      labelInterpolationFnc: function (value) {
        rtf.format(value, 'days');
      }
    }
  })
}
