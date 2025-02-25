document.addEventListener("DOMContentLoaded", function() {
    const urlParams = new URLSearchParams(window.location.search);
    const redirectParam = urlParams.get("redirect");
    const openidButton = document.getElementById("openid-button");
    // If 'redirect' exists and the button is present, update its href
    if (redirectParam && openidButton) {
        openidButton.href = openidButton.href + "?redirect=" + encodeURIComponent(redirectParam);
    }
});
