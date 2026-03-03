async function fetchQuotes() {
    try {
        const response = await fetch('quotes_selected_only.json');
        if (!response.ok) throw new Error('Failed to load quotes');
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching quotes:', error);
        return [];
    }
}

function createQuoteCard(item, index) {
    const quote = item.selected;
    if (!quote) return '';

    const date = new Date(quote.timestamp).toLocaleDateString('ru-RU', {
        day: 'numeric',
        month: 'short',
        year: 'numeric'
    });

    return `
        <article class="quote-card" style="animation-delay: ${index * 0.1}s">
            <div class="quote-text">${quote.text}</div>
            <div class="quote-author">
                <img src="${quote.author_avatar_url}" alt="${quote.author_name}" class="author-avatar" onerror="this.src='media/maskot2.svg'">
                <div class="author-info">
                    <span class="author-name">${quote.author_name}</span>
                    <span class="quote-date">${date}</span>
                </div>
                ${quote.link ? `
                <a href="${quote.link}" target="_blank" class="quote-btn" title="Посмотреть в Discord">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path><polyline points="15 3 21 3 21 9"></polyline><line x1="10" y1="14" x2="21" y2="3"></line></svg>
                </a>` : ''}
            </div>
        </article>
    `;
}

async function init() {
    const quotesGrid = document.getElementById('quotes-grid');
    const data = await fetchQuotes();

    if (data.length === 0) {
        quotesGrid.innerHTML = `
            <div style="grid-column: 1/-1; text-align: center; padding: 10rem 2rem;">
                <h2 style="font-size: 2rem; opacity: 0.5;">Золото смывается... Попробуйте позже.</h2>
            </div>`;
        return;
    }

    // Sort by timestamp descending (newest first)
    const sortedData = data.sort((a, b) => {
        return new Date(b.selected.timestamp) - new Date(a.selected.timestamp);
    });

    quotesGrid.innerHTML = sortedData.map((item, index) => createQuoteCard(item, index)).join('');
}

document.addEventListener('DOMContentLoaded', init);
