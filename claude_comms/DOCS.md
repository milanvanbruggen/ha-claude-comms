# Claude Comms

Geeft Claude **alleen-lezen** toegang tot je Signal-berichten. Signal draait hier als gekoppeld apparaat
(zoals Signal Desktop). Berichten worden lokaal op je Home Assistant opgeslagen en na de bewaartermijn verwijderd.
Er zit bewust geen functie in om berichten te versturen, te wijzigen of te verwijderen.

## Installeren
1. Configuratie: vul `mcp_token` in met een willekeurige string van minimaal 32 tekens
   (bijv. in de terminal: `openssl rand -hex 24`). Bewaar hem, je hebt hem straks nodig.
2. Start de add-on. De eerste keer bouwen duurt op een Pi 5 enkele minuten.
3. Open de add-on (*Open web-UI*) op een **tweede scherm** (laptop/tablet).
4. Op je telefoon: Signal → Instellingen → Gekoppelde apparaten → Apparaat koppelen → scan de QR-code.
5. Ververs de pagina: je ziet *Gekoppeld aan +31…*. Vanaf nu komen nieuwe berichten binnen.
   Oude berichten van vóór het koppelen haalt Signal niet op.

## Claude laten verbinden
De MCP-endpoint luistert op poort 8099, pad `/mcp/<mcp_token>`, binnen het interne netwerk van Home Assistant.
Zet er een Cloudflare Tunnel voor (Cloudflared-add-on) met als service
`http://<hostnaam van deze add-on>:8099` (de hostnaam staat op de infopagina van de add-on).
Voeg in Claude een eigen connector toe met `https://<jouw-subdomein>/mcp/<mcp_token>`.

Alleen het `/mcp/<token>`-pad is via de tunnel bruikbaar. De statuspagina en QR-code werken alleen via Home Assistant (Ingress).

## Tools
- `signal_waiting_for_reply` – gesprekken waarin het laatste bericht niet van jou is
- `signal_read_conversation` – een gesprek lezen
- `signal_recent_conversations` – activiteit van de afgelopen uren
- `signal_search` – zoeken in opgeslagen berichten
- `signal_status` – gezondheid van de koppeling

## Let op
- De token in de URL is je enige sleutel. Lekt hij uit, verander hem dan in de configuratie en in Claude.
- Back-ups van Home Assistant bevatten deze add-on, inclusief de Signal-sleutels en berichten. Bewaar back-ups veilig.
- Ontkoppel in noodgevallen via Signal op je telefoon → Gekoppelde apparaten.
