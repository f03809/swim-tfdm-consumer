(function () {
  const html = document.documentElement;
  const toggle = document.getElementById('theme-toggle');

  function applyTheme(theme) {
    if (theme === 'dark') {
      html.classList.add('dark');
      html.setAttribute('data-theme', 'dark');
    } else {
      html.classList.remove('dark');
      html.setAttribute('data-theme', 'light');
    }
  }

  function loadTheme() {
    const saved = localStorage.getItem('swim-tfdm-theme');
    if (saved === 'light' || saved === 'dark') {
      applyTheme(saved);
    } else {
      applyTheme('dark');
    }
  }

  function toggleTheme() {
    const isDark = html.classList.contains('dark') || html.getAttribute('data-theme') === 'dark';
    const newTheme = isDark ? 'light' : 'dark';
    applyTheme(newTheme);
    localStorage.setItem('swim-tfdm-theme', newTheme);
    if (typeof lucide !== 'undefined') {
      lucide.createIcons();
    }
  }

  loadTheme();

  if (toggle) {
    toggle.addEventListener('click', toggleTheme);
  }

  if (typeof lucide !== 'undefined') {
    lucide.createIcons();
  } else {
    document.addEventListener('DOMContentLoaded', function () {
      if (typeof lucide !== 'undefined') {
        lucide.createIcons();
      }
    });
  }
})();
