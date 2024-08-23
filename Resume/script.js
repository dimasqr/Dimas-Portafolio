const tabsContainer = document.querySelector(".button-group");
const tabs = document.querySelectorAll(".work_experience__tab");
const tabsContent = document.querySelectorAll(".work_experience__content");

/* SMOOTH SCROLL DOWN */
document.addEventListener("DOMContentLoaded", function () {
  const scrollLinks = document.querySelectorAll(".nav_link");

  scrollLinks.forEach(function (link) {
    link.addEventListener("click", function (e) {
      e.preventDefault();
      const targetId = this.getAttribute("href");
      const targetSection = document.querySelector(targetId);

      if (targetSection) {
        targetSection.scrollIntoView({ behavior: "smooth" });
      }
    });
  });
});

/* KEEP THE HEADER VISIBLE AFTER SCROLLING DOWN */
document.addEventListener("DOMContentLoaded", function () {
  const nav = document.querySelector(".nav");
  const header = document.querySelector(".header");
  const navHeight = nav.getBoundingClientRect().height;

  const stickyNav = function (entries) {
    const [entry] = entries;
    if (!entry.isIntersecting) nav.classList.add("sticky");
    else nav.classList.remove("sticky");
  };

  const headerObserver = new IntersectionObserver(stickyNav, {
    root: null,
    threshold: 0,
    rootMargin: `-${navHeight}px`,
  });

  headerObserver.observe(header); // make visible the header
});

/* WORK EXPERIENCE BUTTONS */

tabsContainer.addEventListener("click", function (e) {
  const clicked = e.target.closest(".work_experience__tab");

  // Guard clause
  if (!clicked) return;

  // Remove active classes
  tabs.forEach((t) => t.classList.remove("experience__tab--active"));
  tabsContent.forEach((c) =>
    c.classList.remove("work_experience__content--active")
  );

  // Activate tab
  clicked.classList.add("experience__tab--active");

  // Activate content area
  document
    .querySelector(`.work_experience__content--${clicked.dataset.tab}`)
    .classList.add("work_experience__content--active");
});
