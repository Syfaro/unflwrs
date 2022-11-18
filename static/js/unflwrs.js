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

const deleteAccountBtn = document.getElementById('delete-account');
if (deleteAccountBtn) {
  deleteAccountBtn.addEventListener('click', (ev) => {
    ev.preventDefault();
    deleteAccountBtn.setAttribute('aria-busy', true);

    if (confirm('Are you sure? This will immediately remove all your data.')) {
      const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('value');

      fetch('/account/delete', {
        method: 'post',
        body: new URLSearchParams({
          'csrf': csrfToken,
        }),
        headers: {
          'content-type': 'application/x-www-form-urlencoded'
        }
      }).then(() => {
        window.location.href = '/';
      }).catch((err) => {
        console.error(err);
        alert('Could not delete account.');
        deleteAccountBtn.removeAttribute('aria-busy');
      });
    } else {
      deleteAccountBtn.removeAttribute('aria-busy');
    }
  })
}

if (document.querySelector('main[data-page="export"]')) {
  const startButton = document.getElementById('start-button');
  const exportEvents = document.getElementById('export-events');

  startButton.addEventListener('click', () => {
    startButton.setAttribute('aria-busy', 'true');
    startButton.replaceChildren(document.createTextNode('working'));
  });

  const sse = new EventSource('/export/events');

  function endExport(sse, startButton) {
    sse.close();

    startButton.removeAttribute('aria-busy');
    startButton.setAttribute('disabled', 'true');
    startButton.classList.add('outline', 'secondary');
    startButton.replaceChildren(document.createTextNode('finished'));
  }

  sse.addEventListener('open', () => {
    exportEvents.replaceChildren();
  });

  sse.addEventListener('progress', (ev) => {
    console.debug(ev);

    var eventText;

    const data = JSON.parse(ev.data);
    switch (data['event']) {
      case 'started':
        eventText = 'export started';
        break;
      case 'message':
        eventText = data['data']['text'];
        break;
      case 'error':
        eventText = data['data']['message'];
        alert(`error! ${data['data']['message']}`);
        endExport(sse, startButton);
        break;
      case 'completed':
        eventText = 'completed';
        endExport(sse, startButton);
        break;
    }

    const elem = document.createElement('p');
    elem.appendChild(document.createTextNode(eventText));
    exportEvents.prepend(elem);
  });

  sse.addEventListener('close', () => {
    console.log('closed sse');

    endExport(sse, startButton);
  });
}
